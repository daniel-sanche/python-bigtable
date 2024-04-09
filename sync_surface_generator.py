# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import annotations

import inspect
import ast
import textwrap
import time
import queue
import os
import threading
import concurrent.futures
import importlib

from black import format_str, FileMode
import autoflake
"""
This module allows us to generate a synchronous API surface from our asyncio surface.
"""

class AsyncToSyncTransformer(ast.NodeTransformer):
    """
    This class is used to transform async classes into sync classes.
    Generated classes are abstract, and must be subclassed to be used.
    This is to ensure any required customizations from 
    outside of this autogeneration system are always applied
    """

    def __init__(self, *, name=None, import_replacements=None, name_replacements=None, drop_methods=None, pass_methods=None, error_methods=None):
        """
        Args:
          - name: the name of the class being processed. Just used in exceptions
          - import_replacements: dict of (module, name) to (module, name) replacement import statements
                For example, {("foo", "bar"): ("baz", "qux")} will replace "from foo import bar" with "from baz import qux"
          - name_replacements: dict of names to replace directly in the source code and docstrings
          - drop_methods: list of method names to drop from the class
          - pass_methods: list of method names to replace with "pass" in the class
          - error_methods: list of method names to replace with "raise NotImplementedError" in the class
        """
        self.name = name
        self.import_replacements = import_replacements or {}
        self.name_replacements = name_replacements or {}
        self.drop_methods = drop_methods or []
        self.pass_methods = pass_methods or []
        self.error_methods = error_methods or []

    def update_docstring(self, docstring):
        """
        Update docstring to replace any key words in the name_replacements dict
        """
        if not docstring:
            return docstring
        for key_word, replacement in self.name_replacements.items():
            docstring = docstring.replace(f" {key_word} ", f" {replacement} ")
        if "\n" in docstring:
            # if multiline docstring, add linebreaks to put the """ on a separate line
            docstring = "\n" + docstring + "\n\n"
        return docstring

    def visit_FunctionDef(self, node):
        """
        Re-use replacement logic for Async functions
        """
        return self.visit_AsyncFunctionDef(node)

    def visit_AsyncFunctionDef(self, node):
        """
        Replace async functions with sync functions
        """
        # replace docstring
        docstring = self.update_docstring(ast.get_docstring(node))
        if isinstance(node.body[0], ast.Expr) and isinstance(
            node.body[0].value, ast.Str
        ):
            node.body[0].value.s = docstring
        # drop or replace body as needed
        if node.name in self.drop_methods:
            return None
        elif node.name in self.pass_methods:
            # replace with pass
            node.body = [ast.Expr(value=ast.Str(s="Implementation purposely removed in sync mode"))]
        elif node.name in self.error_methods:
            self._create_error_node(node, "Function marked as unsupported in sync mode")
        else:
            # check if the function contains non-replaced usage of asyncio
            func_ast = ast.parse(ast.unparse(node))
            for n in ast.walk(func_ast):
                if isinstance(n, ast.Call) \
                    and isinstance(n.func, ast.Attribute) \
                    and isinstance(n.func.value, ast.Name) \
                    and n.func.value.id == "asyncio" \
                    and f"asyncio.{n.func.attr}" not in self.import_replacements:
                        path_str = f"{self.name}.{node.name}" if self.name else node.name
                        raise RuntimeError(
                            f"{path_str} contains unhandled asyncio calls: {n.func.attr}. Add method to drop_methods, pass_methods, or error_methods to handle safely."
                        )
        # remove pytest.mark.asyncio decorator
        if hasattr(node, "decorator_list"):
            is_asyncio_decorator = lambda d: all(x in ast.dump(d) for x in ["pytest", "mark", "asyncio"])
            node.decorator_list = [
                d for d in node.decorator_list if not is_asyncio_decorator(d)
            ]
        return ast.copy_location(
            ast.FunctionDef(
                self.name_replacements.get(node.name, node.name),
                self.visit(node.args),
                [self.visit(stmt) for stmt in node.body],
                [self.visit(stmt) for stmt in node.decorator_list],
                node.returns and self.visit(node.returns),
            ),
            node,
        )

    def visit_Call(self, node):
        # name replacement for class method calls
        if isinstance(node.func, ast.Attribute) and isinstance(
            node.func.value, ast.Name
        ):
            node.func.value.id = self.name_replacements.get(node.func.value.id, node.func.value.id)
        return ast.copy_location(
            ast.Call(
                self.visit(node.func),
                [self.visit(arg) for arg in node.args],
                [self.visit(keyword) for keyword in node.keywords],
            ),
            node,
        )

    def visit_Await(self, node):
        return self.visit(node.value)

    def visit_Attribute(self, node):
        if (
            isinstance(node.value, ast.Name)
            and isinstance(node.value.ctx, ast.Load)
            and node.value.id == "asyncio"
            and f"asyncio.{node.attr}" in self.import_replacements
        ):
            replacement = self.import_replacements[f"asyncio.{node.attr}"]
            return ast.copy_location(ast.parse(replacement, mode="eval").body, node)
        elif isinstance(node, ast.Attribute) and node.attr in self.name_replacements:
            new_node = ast.copy_location(
                ast.Attribute(node.value, self.name_replacements[node.attr], node.ctx), node
            )
            return new_node
        return node

    def visit_Name(self, node):
        node.id = self.name_replacements.get(node.id, node.id)
        return node

    def visit_AsyncFor(self, node):
        return ast.copy_location(
            ast.For(
                self.visit(node.target),
                self.visit(node.iter),
                [self.visit(stmt) for stmt in node.body],
                [self.visit(stmt) for stmt in node.orelse],
            ),
            node,
        )

    def visit_AsyncWith(self, node):
        return ast.copy_location(
            ast.With(
                [self.visit(item) for item in node.items],
                [self.visit(stmt) for stmt in node.body],
            ),
            node,
        )

    def visit_ListComp(self, node):
        # replace [x async for ...] with [x for ...]
        new_generators = []
        for generator in node.generators:
            if generator.is_async:
                new_generators.append(
                    ast.copy_location(
                        ast.comprehension(
                            self.visit(generator.target),
                            self.visit(generator.iter),
                            [self.visit(i) for i in generator.ifs],
                            False,
                        ),
                        generator,
                    )
                )
            else:
                new_generators.append(generator)
        node.generators = new_generators
        return ast.copy_location(
            ast.ListComp(
                self.visit(node.elt),
                [self.visit(gen) for gen in node.generators],
            ),
            node,
        )

    def visit_Subscript(self, node):
        if (
            hasattr(node, "value")
            and isinstance(node.value, ast.Name)
            and node.value.id == "AsyncGenerator"
            and self.name_replacements.get(node.value.id, "") == "Generator"
        ):
            # Generator has different argument signature than AsyncGenerator
            return ast.copy_location(
                ast.Subscript(
                    ast.Name("Generator"),
                    ast.Index(
                        ast.Tuple(
                            [
                                self.visit(i)
                                for i in node.slice.elts + [ast.Constant("Any")]
                            ]
                        )
                    ),
                    node.ctx,
                ),
                node,
            )
        elif (
            hasattr(node, "value")
            and isinstance(node.value, ast.Name)
            and self.name_replacements.get(node.value.id, False) is None
        ):
            # needed for Awaitable
            return self.visit(node.slice)
        return ast.copy_location(
            ast.Subscript(
                self.visit(node.value),
                self.visit(node.slice),
                node.ctx,
            ),
            node,
        )

    @staticmethod
    def _create_error_node(node, error_msg):
        # replace function body with NotImplementedError
        exc_node = ast.Call(
            func=ast.Name(id="NotImplementedError", ctx=ast.Load()),
            args=[ast.Str(s=error_msg)],
            keywords=[],
        )
        raise_node = ast.Raise(exc=exc_node, cause=None)
        node.body = [raise_node]


    def get_imports(self, filename):
        """
        Get the imports from a file, and do a find-and-replace against import_replacements
        """
        imports = set()
        with open(filename, "r") as f:
            full_tree = ast.parse(f.read(), filename)
            for node in ast.walk(full_tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    for alias in node.names:
                        if isinstance(node, ast.Import):
                            # import statments
                            new_import = self.import_replacements.get(alias.name, alias.name)
                            imports.add(ast.parse(f"import {new_import}").body[0])
                        else:
                            # import from statements
                            # break into individual components
                            full_path = f"{node.module}.{alias.name}"
                            if full_path in self.import_replacements:
                                full_path = self.import_replacements[full_path]
                            module, name = full_path.rsplit(".", 1)
                            # don't import from same file
                            if module == ".":
                                continue
                            asname_str = f" as {alias.asname}" if alias.asname else ""
                            imports.add(
                                ast.parse(f"from {module} import {name}{asname_str}").body[
                                    0
                                ]
                            )
        return imports

def transform_class(in_obj: Type, **kwargs):
    filename = inspect.getfile(in_obj)
    lines, lineno = inspect.getsourcelines(in_obj)
    ast_tree = ast.parse(textwrap.dedent("".join(lines)), filename)
    new_name = None
    if ast_tree.body and isinstance(ast_tree.body[0], ast.ClassDef):
        # update name
        old_name = ast_tree.body[0].name
        # set default name for new class if unset
        new_name = kwargs.pop("autogen_sync_name", f"{old_name}_SyncGen")
        ast_tree.body[0].name = new_name
        ast.increment_lineno(ast_tree, lineno - 1)
        # add ABC as base class
        ast_tree.body[0].bases = ast_tree.body[0].bases + [
            ast.Name("ABC", ast.Load()),
        ]
    # remove top-level imports if any. Add them back later
    ast_tree.body = [n for n in ast_tree.body if not isinstance(n, (ast.Import, ast.ImportFrom))]
    # transform
    transformer = AsyncToSyncTransformer(name=new_name, **kwargs)
    transformer.visit(ast_tree)
    # find imports
    imports = transformer.get_imports(filename)
    imports.add(ast.parse("from abc import ABC").body[0])
    # add globals
    # for g in transformer.globals:
    #     imports.add(ast.parse(f"import {g}").body[0])
    # add locals from file, in case they are needed
    if ast_tree.body and isinstance(ast_tree.body[0], ast.ClassDef):
        file_basename = os.path.splitext(os.path.basename(filename))[0]
        with open(filename, "r") as f:
            for node in ast.walk(ast.parse(f.read(), filename)):
                if isinstance(node, ast.ClassDef):
                    imports.add(
                        ast.parse(
                            f"from google.cloud.bigtable.{file_basename} import {node.name}"
                        ).body[0]
                    )
    return ast_tree.body, imports


def transform_from_config(config_dict: dict):
    # initialize new tree and import list
    combined_tree = ast.parse("")
    combined_imports = set()
    # process each class
    for class_dict in config_dict["classes"]:
        # convert string class path into class object
        module_path, class_name = class_dict.pop("path").rsplit(".", 1)
        class_object = getattr(importlib.import_module(module_path), class_name)
        # add globals to class_dict
        class_dict["import_replacements"] = {**config_dict.get("import_replacements", {}), **class_dict.get("import_replacements", {})}
        class_dict["name_replacements"] = {**config_dict.get("name_replacements", {}), **class_dict.get("name_replacements", {})}
        # transform class
        tree_body, imports = transform_class(class_object, **class_dict)
        # update combined data
        combined_tree.body.extend(tree_body)
        combined_imports.update(imports)
    # add extra imports
    # if add_imports:
    #     for import_str in add_imports:
    #         combined_imports.add(ast.parse(import_str).body[0])
    # render tree as string of code
    import_unique = list(set([ast.unparse(i) for i in combined_imports]))
    import_unique.sort()
    google, non_google = [], []
    for i in import_unique:
        if "google" in i:
            google.append(i)
        else:
            non_google.append(i)
    import_str = "\n".join(non_google + [""] + google)
    # append clean tree
    header = """# Copyright 2024 Google LLC
    #
    # Licensed under the Apache License, Version 2.0 (the "License");
    # you may not use this file except in compliance with the License.
    # You may obtain a copy of the License at
    #
    #     http://www.apache.org/licenses/LICENSE-2.0
    #
    # Unless required by applicable law or agreed to in writing, software
    # distributed under the License is distributed on an "AS IS" BASIS,
    # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    # See the License for the specific language governing permissions and
    # limitations under the License.
    #
    # This file is automatically generated by sync_surface_generator.py. Do not edit.
    """
    full_code = f"{header}\n\n{import_str}\n\n{ast.unparse(combined_tree)}"
    full_code = autoflake.fix_code(full_code, remove_all_unused_imports=True)
    formatted_code = format_str(full_code, mode=FileMode())
    return formatted_code


if __name__ == "__main__":
    config = {
        "import_replacements": {
            "asyncio.sleep": "time.sleep",
            "asyncio.Queue": "queue.Queue",
            "asyncio.Condition": "threading.Condition",
            "asyncio.Future": "concurrent.futures.Future",
            "google.api_core.retry_async": "google.api_core.retry",
            "typing.AsyncIterable": "typing.Iterable",
            "typing.AsyncIterator": "typing.Iterator",
            "typing.AsyncGenerator": "typing.Generator",
            "grpc.aio.Channel": "grpc.Channel",
            # "google.cloud.bigtable_v2.services.bigtable.async_client.BigtableAsyncClient": "google.cloud.bigtable_v2.services.bigtable.client.BigtableClient",
            # "google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio.PooledBigtableGrpcAsyncIOTransport": "google.cloud.bigtable_v2.services.bigtable.transports.BigtableGrpcTransport",
        }, # replace imports with corresponding sync version. Does not touch the code, only import lines
        "name_replacements": {
            "__anext__": "__next__",
            "__aiter__": "__iter__",
            "__aenter__": "__enter__",
            "__aexit__": "__exit__",
            "aclose": "close",
            "AsyncIterable": "Iterable",
            "AsyncIterator": "Iterator",
            "AsyncGenerator": "Generator",
            "StopAsyncIteration": "StopIteration",
            "BigtableAsyncClient": "BigtableClient",
            "AsyncRetry": "Retry",
            "PooledBigtableGrpcAsyncIOTransport": "BigtableGrpcTransport",
            "Awaitable": None,
            "pytest_asyncio": "pytest",
            "AsyncMock": "mock.Mock",
            # "_ReadRowsOperation": "_ReadRowsOperation_Sync",
            # "Table": "Table_Sync",
            # "BigtableDataClient": "BigtableDataClient_Sync",
            # "ReadRowsIterator": "ReadRowsIterator_Sync",
            # "_MutateRowsOperation": "_MutateRowsOperation_Sync",
            # "MutationsBatcher": "MutationsBatcher_Sync",
            # "_FlowControl": "_FlowControl_Sync",
        },  # performs find/replace for these terms in docstrings and generated code
        "classes": [
            {
                "path": "google.cloud.bigtable.data._async._read_rows._ReadRowsOperationAsync",
                "autogen_sync_name": "_ReadRowsOperation_SyncGen",
                "pass_methods": [],
                "drop_methods": [],
                "error_methods": [],
            },
            {
                "path": "google.cloud.bigtable.data._async.mutations_batcher._MutateRowsOperationAsync",
                "autogen_sync_name": "_MutateRowsOperation_SyncGen",
            },
            {
                "path": "google.cloud.bigtable.data._async.mutations_batcher.MutationsBatcherAsync",
                "autogen_sync_name": "MutationsBatcher_SyncGen",
                "pass_methods": ["_start_flush_timer", "close", "_create_bg_task", "_wait_for_batch_results"]
            },
            {
                "path": "google.cloud.bigtable.data._async.mutations_batcher._FlowControlAsync",
                "autogen_sync_name": "_FlowControl_SyncGen",
            },
            {
                "path": "google.cloud.bigtable.data._async.client.BigtableDataClientAsync",
                "autogen_sync_name": "BigtableDataClient_SyncGen",
                "pass_methods": ["_start_background_channel_refresh", "close", "_ping_and_warm_instances"]
            },
            {
                "path": "google.cloud.bigtable.data._async.client.TableAsync",
                "autogen_sync_name": "Table_SyncGen",
                "pass_methods": ["__init__", "read_rows_sharded"]
            },
        ]
    }

    save_path = "./google/cloud/bigtable/data/_sync/_autogen.py"

    code = transform_from_config(config)

    if save_path is not None:
        with open(save_path, "w") as f:
            f.write(code)


