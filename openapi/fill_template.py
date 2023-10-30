from lorem_text import lorem
import json

EXAMPLES_FOLDER = "./examples"
RESULTS_FOLDER = "./results"
BASIC_TYPES = ["string", "number", "integer", "boolean", "array"]


def read_open_api_file_contents(filename):
    with open(EXAMPLES_FOLDER + "/" + filename, "r") as open_api_file:
        open_api_contents = open_api_file.read()
        return open_api_contents


def save_api_file_contents(filename, json_result):
    with open(RESULTS_FOLDER + "/" + filename, "w") as open_api_file:
        open_api_file.write(json.dumps(json_result))


def generate_according_to_type(param):
    if "schema" in param and "type" in param["schema"] and param["schema"]["type"] in BASIC_TYPES:
        schema_type = param["schema"]["type"]
        if schema_type == "string":
            param["example"] = lorem.words(1)
        elif schema_type == "number":
            param["example"] = 2.71
        elif schema_type == "integer":
            param["example"] = 10
        elif schema_type == "boolean":
            param["example"] = True
        elif schema_type == "array":
            param["example"] = []


def main(filename):
    api_file_contents = read_open_api_file_contents(filename)
    api_data = json.loads(api_file_contents)

    for path, path_info in api_data.get("paths", {}).items():
        for operation, operation_info in path_info.items():
            parameters = operation_info.get("parameters", [])
            for param in parameters:
                if "example" not in param:
                    generate_according_to_type(param)

    save_api_file_contents(filename, api_data)

                
if __name__ == "__main__":
    filename = input("Filename from examples folder: ")
    try:
        main(filename)
    except Exception as ex:
        print(f"Exception is raised: {ex}")

