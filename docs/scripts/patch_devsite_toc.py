# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
This script will run after ``nox -s docfx`` is run. docfx is the api doc format used by 
google cloud. It is described here: https://github.com/googleapis/docuploader?tab=readme-ov-file#requirements-for-docfx-yaml-tarballs.

One of the file used by docfx is toc.yml which is used to generate the table of contents sidebar.
This script will patch file to create subfolders for each of the clients
"""


import yaml
import os
import shutil

# set working directory to /docs
os.chdir(f"{os.path.dirname(os.path.abspath(__file__))}/{os.pardir}")


def add_sections(toc_file_path, section_list, output_file_path=None):
    """
    Add new sections to the autogenerated docfx table of contents file

    Takes in a list of TocSection objects, which should point to a directory of rst files
    within the main /docs directory, which represents a self-contained section of content

    :param toc_file_path: path to the autogenerated toc file
    :param section_list: list of TocSection objects to add
    :param output_file_path: path to save the updated toc file. If None, save to the input file
    """
    # remove any sections that are already in the toc
    remove_sections(toc_file_path, [section.title for section in section_list])
    # add new sections
    current_toc = yaml.safe_load(open(toc_file_path, "r"))
    for section in section_list:
        print(f"Adding section {section.title}...")
        current_toc[0]["items"].insert(-1, section.to_dict())
        section.copy_markdown()
    # save file
    if output_file_path is None:
        output_file_path = toc_file_path
    with open(output_file_path, "w") as f:
        yaml.dump(current_toc, f)


def remove_sections(toc_file_path, section_list, output_file_path=None):
    """
    Remove sections from the autogenerated docfx table of contents file

    Takes in a list of string section names to remove from the toc file

    :param toc_file_path: path to the autogenerated toc file
    :param section_list: list of section names to remove
    :param output_file_path: path to save the updated toc file. If None, save to the input file
    """
    current_toc = yaml.safe_load(open(toc_file_path, "r"))
    print(f"Removing sections {section_list}...")
    new_items = [d for d in current_toc[0]["items"] if d["name"] not in section_list]
    current_toc[0]["items"] = new_items
    # save file
    if output_file_path is None:
        output_file_path = toc_file_path
    with open(output_file_path, "w") as f:
        yaml.dump(current_toc, f)


class TocSection:
    def __init__(self, dir_name, index_file_name):
        """
        :param dir_name: name of the directory containing the rst files
        :param index_file_name: name of an index file within dir_name. This file
            will not be included in the table of contents, but provides an ordered
            list of the other files which should be included
        """
        self.dir_name = dir_name
        self.index_file_name = index_file_name
        index_file_path = os.path.join(dir_name, index_file_name)
        # find set of files referenced by the index file
        with open(index_file_path, "r") as f:
            self.title = f.readline().strip()
            in_toc = False
            self.items = []
            for line in f:
                # ignore empty lines
                if not line.strip():
                    continue
                if line.startswith(".. toctree::"):
                    in_toc = True
                    continue
                # ignore directives
                if ":" in line:
                    continue
                if not in_toc:
                    continue
                # bail when toc indented block is done
                if not line.startswith(" ") and not line.startswith("\t"):
                    break
                # extract entries
                self.items.append(self.extract_toc_entry(line.strip()))

    def extract_toc_entry(self, file_name):
        """
        Given the name of a file, extract the title and href for the toc entry,
        and return as a dictionary
        """
        # load the file to get the title
        with open(f"{self.dir_name}/{file_name}.rst", "r") as f2:
            file_title = f2.readline().strip()
            return {"name": file_title, "href": f"{file_name}.md"}

    def to_dict(self):
        """
        Convert the TocSection object to a dictionary that can be written to a yaml file
        """
        return {"name": self.title, "items": self.items}

    def copy_markdown(self):
        """
        Copy markdown files from _build/markdown/dir_name to _build/html/docfx_yaml

        This is necessary because the markdown files in sub-directories
        are not copied over by the docfx build by default
        """
        for file in os.listdir("_build/markdown/" + self.dir_name):
            shutil.copy(
                f"_build/markdown/{self.dir_name}/{file}",
                f"_build/html/docfx_yaml",
            )


def validate_toc(toc_file_path, expected_section_list, added_sections):
    current_toc = yaml.safe_load(open(toc_file_path, "r"))
    # make sure the set of sections matches what we expect
    found_sections = [d["name"] for d in current_toc[0]["items"]]
    assert found_sections == expected_section_list
    # make sure each customs ection is in the toc
    for section in added_sections:
        assert section.title in found_sections
    # make sure each rst file in each custom section dir is listed in the toc
    for section in added_sections:
        items_in_toc = [
            d["items"]
            for d in current_toc[0]["items"]
            if d["name"] == section.title and ".rst"
        ][0]
        items_in_dir = [f for f in os.listdir(section.dir_name) if f.endswith(".rst")]
        # subtract 1 for index
        assert len(items_in_toc) == len(items_in_dir) - 1
        for file in items_in_dir:
            if file != section.index_file_name:
                base_name, _ = os.path.splitext(file)
                assert any(d["href"] == f"{base_name}.md" for d in items_in_toc)
    # make sure the markdown files are present in the docfx_yaml directory
    for section in added_sections:
        items_in_toc = [
            d["items"]
            for d in current_toc[0]["items"]
            if d["name"] == section.title and ".rst"
        ][0]
        md_files = [d["href"] for d in items_in_toc]
        for file in md_files:
            assert os.path.exists(f"_build/html/docfx_yaml/{file}")
    print("Toc validation passed")


if __name__ == "__main__":
    # Add secrtions for the async_data_client and classic_client directories
    toc_path = "_build/html/docfx_yaml/toc.yml"
    custom_sections = [
        TocSection(
            dir_name="async_data_client", index_file_name="async_data_usage.rst"
        ),
        TocSection(dir_name="classic_client", index_file_name="usage.rst"),
    ]
    add_sections(toc_path, custom_sections)
    # Remove the Bigtable section, since it has duplicated data
    remove_sections(toc_path, ["Bigtable"])
    # run validation to make sure yaml is structured as we expect
    validate_toc(
        toc_file_path=toc_path,
        expected_section_list=[
            "Overview",
            "bigtable APIs",
            "Changelog",
            "Multiprocessing",
            "Async Data Client",
            "Classic Client",
        ],
        added_sections=custom_sections,
    )