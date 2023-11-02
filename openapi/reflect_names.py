import ast
import codegen

GENERATE_RANDOMS_FOO_NAME = "generate_according_to_type"
"""		
	~
	THE TASK
	
	Use a reflection mechanism for parsing the function 
	of generation STRING examples for the open-api 
	and change it to the custom

"""


def read_file_contents(filename):
	with open(filename, "r") as py_file:
		py_code_contents = py_file.read()
		return py_code_contents


def write_file_contents(filename, ast_result):
	with open(filename, "w") as py_file:
		py_file.write(ast_result)


def get_name_base(filename):
	return filename.split(".")[0]


def compose_2vpy_filename(filebase):
	return filebase + "_v2.py"


def parse_random_value_gen_function(ast_foo_node, subst_value):
	for ast_node in ast_foo_node.body:
		if isinstance(ast_node, ast.FunctionDef) and ast_node.name == GENERATE_RANDOMS_FOO_NAME:
			ast_node.body[0].body[1].body[0].value = subst_value;
			#						 |   |		|		|
			#						 foo if  	if		assign(=)
			#							 |		|		|
			#							 (check for schema)
			#									|		|
			#									(check for type)
			#											|
			#											(assing random lorem generation)
			


def substitute_ast_str_val(py_code_contents, subst_value):
	ast_contents = ast.parse(py_code_contents)
	parse_random_value_gen_function(ast_contents, subst_value)
	return ast_contents


def main():
	py_filename = input(".py filename: ")
	new_value = input("Enter new constant value: ")
	py_code_contents = read_file_contents(py_filename)
	ast_result = substitute_ast_str_val(py_code_contents, ast.Constant(value=new_value))
	write_file_contents(compose_2vpy_filename(get_name_base(py_filename)), ast.unparse(ast_result))


if __name__ == "__main__":
	main()