import xml.etree.ElementTree as ET
from lxml import etree
from pandas import DataFrame
from pathlib import Path
import logging
from datetime import datetime
import re
import bg3data_lib as bg3lib
from typing import Dict, List, Any, Union, Tuple

#Dict_Table = TypeVar('Dict_Table', bound=Dict[str, Dict[str, Any]])
#List_Str = TypeVar('List_Str', bound=Union[List[str], None])

# Type Aliasses
Dict_Table = Dict[str, Dict[str, Any]]
List_Str = Union[List[str], None]
# #############################################################################################
# START - User options and settings
#
Option_fill_templates_recursively = True
Option_fill_merged_recursively = True
Option_add_timestamp = True
Option_save_to_excel = False
Option_save_to_json_dict = False
Option_save_to_json_array = False
Option_save_to_sqlite = True

# List of languages to use for translations
#TRANSLATION_LANGUAGES = ["English", "French"]
TRANSLATION_LANGUAGES = ["English"]

LOGGING_LEVEL = logging.INFO  # possible values are: 'DEBUG,INFO,WARNING,ERROR,CRITICAL'
UNPACKED_DATA_FOLDER = Path('G:/BG3/Tools/bg3-modders-multitool/UnpackedData')
OUTPUT_FOLDER = Path('G:/BG3/Dev/Output')
#
# END - User options and settings
# #############################################################################################

# Sorted list of folders to process (the order matters!)
# L1 is used for "merged" data
ROOT_FOLDERS_L1: List[str] = [ # List of first level root folders directory (add new game Patch/Version here)
    "Shared", "Gustav",
    "Patch0_Hotfix1", "Patch0_Hotfix2", "Patch0_Hotfix3", "Patch0_Hotfix4",
    "Patch1",
    "Patch2", "Patch2_Hotfix1", "Patch2_Hotfix2",
    "Patch3"
]

ROOT_FOLDERS_L2: List[str] = [ # List of second level root folders directory
    "Public/Shared",
    "Public/SharedDev",
    "Public/Gustav",
    "Public/GustavDev"
]

MERGED_EXCLUDE_FOLDERS = [ # List if directories to exclude from merged files processing
    '[PAK]', 'Assets', 'Content'
 ]

STATS_FILES = [ # List of files to process within STATS folders
    ["Armors", "Armor.txt"],
    ["Weapons", "Weapon.txt"],
    ["Objects", "Object.txt"],
    ["Passives", "Passive.txt"],
    ["Spells", "Spell_*.txt"],
    ["Statuses", "Status_*.txt"],
    ["Characters", "Character.txt"],
    ["CriticalHitTypes", "CriticalHitTypes.txt"],
    ["Interrupts", "Interrupt.txt"]
]

TEMPLATES_FOLDERS = [ # Sorted list of template folders to process within ROOT TEMPLATES
    "RootTemplates",
    "TimelineTemplates"
]

FLAGS_FOLDERS = [ # Sorted list of template folders to process for "flags" data
    "Flags",
]

TAGS_FOLDERS = [ # Sorted list of template folders to process for "tags" data
    "Tags",
]

# Define the Templates class to handle templates files and lookup
class Templates:
    def __init__(self):
        self.data = {}  # initialize template

    def load_templates(self, template_data):
        # Assuming template_data is a dictionary where keys are template IDs and values are template details
        self.data = template_data

    def get_template(self, template_id):
        return self.data.get(template_id, None)

    def get_parent_template(self, template_id):
        template = self.get_template(template_id)
        if template:
            parent_template_id = template.get("ParentTemplateId", None)
            if parent_template_id:
                return self.get_template(parent_template_id)
        return None

# Initialize an instance of the Templates class for persistency
templates = Templates()

# Load template data into the templates instance
def process_templates_folders(output_folder: Path) -> Dict_Table:
    logging.info(f"TEMPLATES Folders - Start processing")
    templates_data: Dict_Table = {}

    root_sub_folders = [
        UNPACKED_DATA_FOLDER / folder_l1 / folder_l2 / sub_folder
        for folder_l1 in ROOT_FOLDERS_L1
        for folder_l2 in ROOT_FOLDERS_L2
        for sub_folder in TEMPLATES_FOLDERS
    ]

    # Now you can iterate through root_sub_folders as needed
    for root_sub_folder in root_sub_folders:
        bg3lib.process_lsx_files(
            UNPACKED_DATA_FOLDER,
            root_sub_folder,
            "**/*.lsx",
            "GameObjects",
            "MapKey",
            templates_data,
        )
    logging.info(f"TEMPLATES - Updating RootTemplates with ParentTemplateName")

    for record_key, nested_dict in templates_data.items():
        parent_template_id = nested_dict.get("ParentTemplateId")
        if parent_template_id:
            try:
                parent_template = templates_data[parent_template_id]
                if parent_template:
                    parent_template_name = parent_template["Name"]
                    nested_dict["ParentTemplateName"] = parent_template_name
                    templates_data[record_key] = nested_dict
                    logging.debug(f"{nested_dict['Name']} --> {parent_template_name}")
            except KeyError as e:
                logging.warning(f"Exception has occurred: {e} -- {nested_dict['Name']}")

    # ordering columns:
    columns_order = ["MapKey", "Name", "Type", "ParentTemplateName"]
    for column in ["DisplayName", "Description"]:
        for language in TRANSLATION_LANGUAGES:
            columns_order.append(f"{column}{language}")
    columns_order.append("ParentTemplateId")
    templates_data = bg3lib.sort_data_dict_columns(templates_data, columns_order)

    if Option_fill_templates_recursively:
        fill_templates_recursively(templates_data)

    # Create a DataFrame from the data dictionary
    #data_frame = pd.DataFrame.from_dict(templates_data, orient="index")

    if Option_save_to_json_dict:
        bg3lib.save_to_json_dict(
            templates_data, output_folder / "Json files (dict)" / f"Templates_dict.json"
        )
    if Option_save_to_json_array:
        bg3lib.save_to_json_array(
            templates_data,
            output_folder / "Json files (array)" / f"Templates_array.json",
        )
    if Option_save_to_excel:
        bg3lib.save_to_excel_with_xlsxwriter_direct(templates_data, output_folder / "Excel Files" / "Templates.xlsx", "Templates", columns_order)
    if Option_save_to_sqlite:
        bg3lib.save_data_dict_to_sqlite(templates_data, output_folder / "bg3data-raw.sqlite3", "Templates", columns_order)

    return templates_data

def fill_templates_recursively(_templates_data: Dict_Table):
    logging.info("fill_template_recursively - Start")

    for template_id, template in _templates_data.items():
        if template.get("TemplateFilled", False):
            continue
        template["TemplateFilled"] = False
        stack: List[Tuple[Dict[str, Any], int]] = [(template, 0)]
        
        while stack:
            current_template, depth = stack.pop()
            parent_template_id = current_template.get("ParentTemplateId")

            if not parent_template_id:
                # this template has no parent ( it is a root!)
                continue

            parent_template = _templates_data.get(parent_template_id)

            if not parent_template:
                # the parent template id cannot be found
                logging.warning(f'Parent template not found, key = "{parent_template_id}"')
                continue

            ChildTemplateCount = parent_template.get("ChildTemplateCount", 0)
            parent_template["ChildTemplateCount"] = ChildTemplateCount + 1

            if not current_template.get("TemplateFilled", False):
                for key, value in parent_template.items():
                    current_template.setdefault(key, value)

                current_template["TemplateFilled"] = True
            # recurse in
            if depth <= 10:
                stack.append((parent_template, depth + 1))
            else:
                logging.error(f"Maximum recursion depth reached for template {current_template}")
        # whole stack poped
        
    logging.info("fill_template_recursively - Finished")

def process_merged_folders(output_folder: Path) -> Dict_Table:

    logging.info(f"MERGED Folders - Start processing")

    # define an empty data dictionary to store the results
    data_dict: Dict_Table = {}

    file_pattern = '_merged.lsf.lsx'

    # Initialize an empty list to store matching files
    # Use rglob directly to recursively search for matching files under the specified folder
    matching_files: List[Any]= []

    for root_folder in ROOT_FOLDERS_L1:
        folder = UNPACKED_DATA_FOLDER / root_folder
        matching_files.extend(folder.rglob(file_pattern))

    # Initialize a count of matching files
    matching_file_count = 0

    # Process each matching file
    for file_path in matching_files:
        # Check if the parent directory contains any excluded directory
        if any(excluded_dir in str(file_path.parent) for excluded_dir in MERGED_EXCLUDE_FOLDERS):
            continue  # Skip this file
        
        matching_folder = file_path.parent
        matching_file_count += 1

        bg3lib.process_lsx_files(UNPACKED_DATA_FOLDER, matching_folder, file_pattern, "GameObjects", "MapKey", data_dict)
    
    logging.info(f"MERGED Folders - Processed {matching_file_count} matching files")

    logging.info(f"MERGED - Updating TemplateName with ParentTemplateName")
    for record_key, nested_dict in data_dict.items():
        parent_template_id = nested_dict.get('TemplateName')
        if parent_template_id:
            parent_template = templates.get_template(parent_template_id)
            if parent_template:
                parent_template_name = parent_template['Name']
                nested_dict['ParentTemplateName'] = parent_template_name
                
                # fill cells from template where applicable
                if Option_fill_merged_recursively:
                    for key, value in parent_template.items():
                        if key in ["ParentTemplatedId", "TemplateFilled", "ChildTemplateCount"]:
                            continue
                        nested_dict.setdefault(key, value)
                
                data_dict[record_key] = nested_dict
                logging.debug(f"{nested_dict['TemplateName']} --> {parent_template_name}")

    # ordering all columns:
    columns_order = ["MapKey", "Name", "Type", "ParentTemplateName"]
    for column in ["DisplayName", "Description"]:
        for language in TRANSLATION_LANGUAGES:
            columns_order.append(f"{column}{language}")
    columns_order.append("ParentTemplate")


    data_dict =  bg3lib.sort_data_dict_columns(data_dict, columns_order)
    
    data_dict_items: Dict_Table = {}
    data_dict_characters: Dict_Table = {}

    for key, value in data_dict.items():
        if value["Type"] == "item":
            data_dict_items[key] = value
        elif value["Type"] == "character":
            data_dict_characters[key] = value


    if Option_save_to_json_dict:
        bg3lib.save_to_json_dict(data_dict, output_folder / "Json files (dict)" / f"Merged_All_dict.json")
        bg3lib.save_to_json_dict(data_dict_items, output_folder / "Json files (dict)" / f"Merged_Items_dict.json")
        bg3lib.save_to_json_dict(data_dict_characters, output_folder / "Json files (dict)" / f"Merged_Characters_dict.json")

    if Option_save_to_json_array:
        bg3lib.save_to_json_array(data_dict, output_folder / "Json files (array)" / f"Merged_array.json")
        bg3lib.save_to_json_array(data_dict_items, output_folder / "Json files (dict)" / f"Merged_Items_dict.json")
        bg3lib.save_to_json_array(data_dict_characters, output_folder / "Json files (dict)" / f"Merged_Characters_dict.json")
     
    if Option_save_to_sqlite:
        data_frame = DataFrame.from_dict(data_dict, orient="index")
        data_frame = bg3lib.merge_columns_with_same_case_insensitive_name(data_frame)
        
        # Creating a different table for each `type` of objects

        unique_types = data_frame['Type'].unique()

        for type_value in unique_types:
            filtered_df = data_frame[data_frame['Type'] == type_value]
            db_table = f"{type_value}"
            bg3lib.save_data_frame_to_sqlite(filtered_df, output_folder / "bg3data-merged.sqlite3", db_table, columns_order)

        # creating a smaller database with only the `Items`  and ` Characters` type in it
        bg3lib.save_data_dict_to_sqlite(data_dict_items, output_folder / "bg3data-merged-small.sqlite3", "Items", columns_order)
        bg3lib.save_data_dict_to_sqlite(data_dict_characters, output_folder / "bg3data-merged-small.sqlite3", "Characters", columns_order)

    if Option_save_to_excel:
        # creating smaller excel files with only the `Items`  and ` Characters` type in it
        bg3lib.save_to_excel_with_xlsxwriter_direct(data_dict_items, output_folder / "Excel Files" / "Merged_Items.xlsx", "Items", columns_order)
        bg3lib.save_to_excel_with_xlsxwriter_direct(data_dict_characters, output_folder / "Excel Files" / "Merged_Characters.xlsx", "Characters", columns_order)

    return data_dict
   
def process_stats_folders(output_folder: Path):
    # Regular expression pattern for matching translation handles
    translations_pattern = re.compile(r'^(?P<Handle>h[a-z0-9]{36}(_\d+)*);(?P<Version>\d+)$')

    def parse_stats_records(input_file, current_folder_path: Path) -> List[Dict[str, Any]]:
        # Function to identify fields that have a translation (DisplayName, Description, TranslatedString) and adding their translations
        def add_translations(current_record, data_key):
            data_value = current_record[data_key]
            if data_key == "RootTemplate":
                found_template_path = None
                templates_directories = [UNPACKED_DATA_FOLDER / folder_l1 / folder_l2 / "RootTemplates"
                                         for folder_l1 in ROOT_FOLDERS_L1
                                         for folder_l2 in ROOT_FOLDERS_L2]
                for current_directory in templates_directories:
                    xml_file_path = current_directory / f"{data_value}.lsf.lsx"
                    if xml_file_path.exists():
                        found_template_path = xml_file_path
                        break
                if found_template_path:
                    parser = etree.XMLParser()
                    tree = etree.parse(found_template_path , parser)
                    root = tree.getroot()
                    game_objects_node = root.find(".//node[@id='GameObjects']")
                    for attribute in game_objects_node.findall(".//attribute[@type='TranslatedString']"):
                        id = attribute.get("id")
                        LocalizedHandle = attribute.get("handle")
                        LocalizedVersion = attribute.get("version")
                        bg3lib.add_localized_text(current_record, id, LocalizedHandle, LocalizedVersion)
            else:
                # Examples:
                #   DisplayName      = "hdfb7c1d1g8c33g4f4fgba15gbaf629742787;1"
                #   Description      = "hbe6c8e01g0c14g46f6g8e12g3568c8c0fb3b;2"
                #   ExtraDescription = "h6a5e31efg2e94g4316ga2d0ge2a980b91b5e;5"
                if data_value:
                    matches = translations_pattern.match(data_value)
                    if matches:
                        id = data_key
                        LocalizedHandle = matches.group('Handle')
                        LocalizedVersion = matches.group('Version')
                        bg3lib.add_localized_text(current_record, id, LocalizedHandle, LocalizedVersion)

        def parse_data_property(literal):
            parts = literal.split('"')
            if len(parts) < 5:
                raise Exception("Stat data entry match error")
            return parts[1], parts[3]

        records: List[Dict[str, Any]] = []
        current_record : Dict[str, Any] = {}
        root_folder = current_folder_path.relative_to(UNPACKED_DATA_FOLDER)

        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        for line in lines:
            line = line.strip()

            if line == "":
                if current_record:
                    current_record['RootFolder'] = str(root_folder)
                    records.append(current_record.copy())
                    current_record = {}
            elif line.startswith("new entry"):
                current_record = {'EntryName': line.split('"')[1]}
            elif line.startswith("type"):
                current_record['EntryType'] = line.split('"')[1]
            elif line.startswith("using"):
                current_record['EntryUsing'] = line.split('"')[1]
            elif line.startswith("data"):
                data_key, data_value = parse_data_property(line)
                if data_value:
                    current_record[data_key] = data_value
                    add_translations(current_record, data_key)
            else:
                logging.warning(f" Unknown key: {line}")

        return records

    logging.info(f"STATS Folders - Start processing")

    for output_sheet, input_file_glob in STATS_FILES:
        data_dict: Dict_Table = {}

        logging.info(f"STATS - Processing {output_sheet}")

        root_folders = [Path(folder_l1) / Path(folder_l2)
                        for folder_l1 in ROOT_FOLDERS_L1
                        for folder_l2 in ROOT_FOLDERS_L2]

        for root_folder in root_folders:
            current_folder_path = UNPACKED_DATA_FOLDER / root_folder
            generated_data_folder = current_folder_path / "Stats/Generated/Data"

            stat_files = generated_data_folder.glob(input_file_glob)

            for stat_file in stat_files:
                logging.debug(
                    f"{stat_file.relative_to(UNPACKED_DATA_FOLDER)} - Processing")
                records = parse_stats_records(stat_file, current_folder_path)

                # Fill data_dict
                for record in records:
                    entry_name = record['EntryName']
                    if entry_name in data_dict:
                        existing_record = data_dict[entry_name]
                        logging.debug(
                            f"Overwriting duplicate entry {entry_name} from {existing_record['RootFolder']} with {root_folder}")
                    data_dict[entry_name] = record

        # --- Moving important columns in front of the records
        columns_order = ["EntryName", "EntryType", "EntryUsing"]
        if output_sheet not in ('Characters', 'CriticalHitTypes'):
            for column in ["DisplayName", "Description"]:
                for language in TRANSLATION_LANGUAGES:
                    columns_order.append(f"{column}{language}")
        if output_sheet in ('Armors', 'Weapons', 'Objects'):
            columns_order.append("RootTemplate")
        columns_order.append("RootFolder")

        data_dict = bg3lib.sort_data_dict_columns(data_dict, columns_order)
        # ---

        logging.info(f"STATS - Processed '{output_sheet}': {len(data_dict)} rows")


    for output_sheet, input_file_glob in STATS_FILES:
        data_dict: Dict_Table = {}

        logging.info(f"STATS - Processing {output_sheet}")

        root_folders = [Path(folder_l1) / Path(folder_l2)
            for folder_l1 in ROOT_FOLDERS_L1
            for folder_l2 in ROOT_FOLDERS_L2]

        for root_folder in root_folders:
            current_folder_path = UNPACKED_DATA_FOLDER / root_folder
            generated_data_folder = current_folder_path / "Stats/Generated/Data"

            stat_files = generated_data_folder.glob(input_file_glob)

            for stat_file in stat_files:
                logging.debug(
                    f"{stat_file.relative_to(UNPACKED_DATA_FOLDER)} - Processing")
                records = parse_stats_records(stat_file, current_folder_path)

                # fill data_dict

                for record in records:
                    entry_name = record['EntryName']
                    if entry_name in data_dict:
                        existing_record = data_dict[entry_name]
                        logging.debug(
                            f"Overwriting duplicate entry {entry_name} from {existing_record['RootFolder']} with {root_folder}")
                    data_dict[entry_name] = record

        # --- Moving important columns in front of the records
        columns_order = ["EntryName", "EntryType", "EntryUsing"]
        if output_sheet not in ('Characters', 'CriticalHitTypes'):
            for column in ["DisplayName", "Description"]:
                for language in TRANSLATION_LANGUAGES:
                    columns_order.append(f"{column}{language}")
        if output_sheet in ('Armors', 'Weapons', 'Objects'):
            columns_order.append("RootTemplate")
        columns_order.append("RootFolder")

        data_dict =  bg3lib.sort_data_dict_columns(data_dict, columns_order)
        # ---

        logging.info(
            f"STATS - Processed {output_sheet}: {len(data_dict)} rows")

        # Create a DataFrame from the data dictionary
        #data_frame = pd.DataFrame.from_dict(data_dict, orient="index")
        '''
        # Initialize an empty list for the reordered columns
        reordered_columns = []

        # Add columns from columns_order if they exist in the DataFrame
        for col in columns_order:
            if col in data_frame.columns:
                reordered_columns.append(col)
        # Add all remaining columns from the DataFrame that were not explicitly in columns_order
        remaining_columns = [
            col for col in data_frame.columns if col not in reordered_columns]
        reordered_columns.extend(remaining_columns)

        # Create the reordered DataFrame
        data_frame = data_frame[reordered_columns]
        '''
        if Option_save_to_json_dict:
            bg3lib.save_to_json_dict(data_dict, output_folder /
                              "Json files (dict)" / f"{output_sheet}_dict.json")
        if Option_save_to_json_array:
            bg3lib.save_to_json_array(data_dict, output_folder /
                               "Json files (array)" / f"{output_sheet}_array.json")
        if Option_save_to_excel:
            bg3lib.save_to_excel_with_xlsxwriter_direct(data_dict, output_folder / "Excel Files" / f"{output_sheet}.xlsx", output_sheet, columns_order)
            bg3lib.save_to_excel_with_openpyxl_direct(data_dict, output_folder / "Excel Files" / "All_Stats.xlsx", output_sheet, columns_order)
        if Option_save_to_sqlite:
            bg3lib.save_data_dict_to_sqlite_direct(data_dict, output_folder / "bg3data-raw.sqlite3", output_sheet, columns_order)

def post_process_tags_and_flags(record: Dict, key_name: str) -> None:
    # Special post processing for adding the RealUID as first column
    root_file = record.get("RootFile")
    if root_file:
        record[key_name] = root_file.split('.', 1)[0]

def process_tags_folders(output_folder: Path) -> Dict_Table:

    logging.info(f"TAGS Folders - Start processing")

    data_dict : Dict_Table = {}

    folders = [
        UNPACKED_DATA_FOLDER / folder_l1 / folder_l2 / sub_folder
        for folder_l1 in ROOT_FOLDERS_L1
        for folder_l2 in ROOT_FOLDERS_L2
        for sub_folder in TAGS_FOLDERS
    ]

    for folder in folders:
            bg3lib.process_lsx_files(UNPACKED_DATA_FOLDER, folder, "**/*.lsx", "Tags", "UUID", data_dict, "Category") #, post_process_tags_and_flags)

    # --- Moving important columns in front of the records
    columns_order = ["UUID", "Name", "Description"]
    for column in ["DisplayName", "DisplayDescription"]:
        for language in TRANSLATION_LANGUAGES:
            columns_order.append(f"{column}{language}")
    columns_order.append("Category")

    data_dict =  bg3lib.sort_data_dict_columns(data_dict, columns_order)

    #data_frame = pd.DataFrame.from_dict(data_dict, orient="index")

    if Option_save_to_json_dict:
        bg3lib.save_to_json_dict(data_dict, output_folder / "Json files (dict)" / f"Tags_dict.json")
    if Option_save_to_json_array:
        bg3lib.save_to_json_array(data_dict, output_folder / "Json files (array)" / f"Tags_array.json")
    if Option_save_to_excel:
        bg3lib.save_to_excel_with_xlsxwriter_direct(data_dict, output_folder / "Excel Files" / "Tags.xlsx", "Tags", columns_order)
    if Option_save_to_sqlite:
        bg3lib.save_data_dict_to_sqlite(data_dict, output_folder / "bg3data-raw.sqlite3", "Tags", columns_order)
    return data_dict

def process_flags_folders(output_folder: Path) -> Dict_Table:

    logging.info(f"FLAGS Folders - Start processing")

    data_dict: Dict_Table = {}


    root_sub_folders = [
            UNPACKED_DATA_FOLDER / folder_l1 / folder_l2 / sub_folder
            for folder_l1 in ROOT_FOLDERS_L1
            for folder_l2 in ROOT_FOLDERS_L2
            for sub_folder in FLAGS_FOLDERS
        ]

    for root_sub_folder in root_sub_folders:
            bg3lib.process_lsx_files(UNPACKED_DATA_FOLDER, root_sub_folder, "**/*.lsx", "Flags", "UUID", data_dict, None) #, post_process_tags_and_flags)

    columns_order = ["UUID", "Name", "Description", "Usage"]
    data_dict =  bg3lib.sort_data_dict_columns(data_dict, columns_order)

    #data_frame = pd.DataFrame.from_dict(data_dict, orient="index")

    if Option_save_to_json_dict:
        bg3lib.save_to_json_dict(data_dict, output_folder /
                          "Json files (dict)" / f"Flags_dict.json")
    if Option_save_to_json_array:
        bg3lib.save_to_json_array(data_dict, output_folder /
                           "Json files (array)" / f"Flags_array.json")
    if Option_save_to_excel:
       bg3lib.save_to_excel_with_xlsxwriter_direct(data_dict, output_folder / "Excel Files" / "Flags.xlsx", "Flags", columns_order)
    if Option_save_to_sqlite:
        bg3lib.save_data_dict_to_sqlite(data_dict, output_folder / "bg3data-raw.sqlite3", "Flags", columns_order)
    return data_dict

# Main function
def main():

    TimeStamp = ''
    if Option_add_timestamp:
        DateTime = datetime.now().strftime('%Y%m%d_%H%M%S')
        TimeStamp = f"_{DateTime}"

    output_folder_stamped = OUTPUT_FOLDER / f"Stats{TimeStamp}"
    output_folder_stamped.mkdir(parents=True, exist_ok=True)

    script_name = Path(__file__).stem
    log_file = output_folder_stamped / f"{script_name}.log"
    bg3lib.enable_logging(log_file,LOGGING_LEVEL)

    logging.info(f"STARTING!")

    logging.warning(
        f"==========> WARNING: the order of folders in ROOT_FOLDERS does matter: records will be overwritten based on that implicit 'loading' order")

    logging.info(f"Loading translations")

    ## ## ## ##
    bg3lib.translator.load_translations(TRANSLATION_LANGUAGES,UNPACKED_DATA_FOLDER)
    ##
    templates_data = process_templates_folders(output_folder_stamped)
    global templates
    templates = Templates()
    templates.load_templates(templates_data)
    ##
    process_merged_folders(output_folder_stamped)
    ##
    process_stats_folders(output_folder_stamped)
    ##
    process_tags_folders(output_folder_stamped)
    ##
    process_flags_folders(output_folder_stamped)
    ## ## ## ##
    logging.info(f"ALL DONE!")

if __name__ == "__main__":
    
    profiling = 2
    if profiling == 0:
        main()

    elif profiling == 1:

        import cProfile, pstats
        with cProfile.Profile() as pr:
            main()
        
        with open('profiling_stats.txt', 'w') as stream:
            stats = pstats.Stats(pr, stream=stream)
            stats.strip_dirs()
            stats.sort_stats('time')
            stats.dump_stats('.prof_stats')
            stats.print_stats()        

    elif profiling == 2:
        import cProfile, pstats
        cProfile.run("main()", "{}.profile".format(__file__))
        stats = pstats.Stats("{}.profile".format(__file__))
        stats.strip_dirs()
        stats.sort_stats("time").print_stats(10)
    else:
        raise SystemExit(f"Invalid profiling option: {profiling}")
        