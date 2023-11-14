from lorem_text import lorem
import json
import re
import sys

EXAMPLES_FOLDER = "./examples"
RESULTS_FOLDER = "./results"
BASIC_TYPES = ["string", "number", "integer", "boolean", "array"]

schemas = {}


def read_open_api_file_contents(filename):
    filename = filename if "examples" in filename else (EXAMPLES_FOLDER + "/" + filename)
    with open(filename, "r") as open_api_file:
        open_api_contents = open_api_file.read()
        return open_api_contents


def save_api_file_contents(filename, json_result):
    filename = filename.split("/")[-1] if "/" in filename else filename
    with open(RESULTS_FOLDER + "/" + filename, "w") as open_api_file:
        open_api_file.write(json.dumps(json_result))


def generate_outside_scheme(param):
    if "type" in param:
        schema_type = param["type"]

def generate_according_to_type(param):
    global schemas
    EXAMPLE_NAME = "sample_example"

    if "schema" not in param:
        return

    if "type" in param["schema"] and param["schema"]["type"] in BASIC_TYPES:
        # print(f"Param: {param}")
        schema_type_value = param['schema']['type']
        param["schema"]["example"] = generate_example_by_type(schema_type_value)
        

    if "$ref" in param["schema"]:
        reference = param["schema"]["$ref"].split("/")[-1]
        if reference not in schemas.keys():
            raise Exception("Error in finding scheme for linked with parameter")
        
        linked_schema = schemas[reference]
        linked_schema_type = linked_schema["type"]
        if linked_schema_type not in BASIC_TYPES:
            return

        parameters["examples"][EXAMPLE_NAME] = {
            value: generate_example_by_type(linked_schema_type)
        }



def generate_example_by_type(schema_type):
    if schema_type == "string":
            return "Kaboom!"
    elif schema_type == "number":
        return 2.71
    elif schema_type == "integer":
        return 1322
    elif schema_type == "boolean":
        return True
    elif schema_type == "array":
        return []



def read_schemas(api_data):
    global schemas
    schemas = api_data["components"]["schemas"]
    # print(schemas.keys())


def main(filename):
    api_file_contents = read_open_api_file_contents(filename)
    api_data = json.loads(api_file_contents)

    # 0 - check version
    if "openapi" not in api_data.keys() or not re.match("3", api_data["openapi"]):
        print("Wrong version of the API")
        return

    # 1 - read all schemas
    read_schemas(api_data)

    for path, path_info in api_data.get("paths", {}).items():
        for operation, operation_info in path_info.items():
            parameters = operation_info.get("parameters", [])
            for param in parameters:
                if "example" not in param:
                    generate_according_to_type(param)

    save_api_file_contents(filename, api_data)

                
if __name__ == "__main__":
    filename = sys.argv[1] if sys.argv[1] else input("Filename from examples folder: ")
    try:
        main(filename)
    except Exception as ex:
        print(f"Exception is raised: {ex}")

