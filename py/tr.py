from transformers import MarianMTModel, MarianTokenizer
import datetime
import sys


model_name = "Helsinki-NLP/opus-mt-uk-ru"
model = MarianMTModel.from_pretrained(model_name)
tokenizer = MarianTokenizer.from_pretrained(model_name)

def trans(text):
	inputs = tokenizer(text, return_tensors="pt", truncation=True)
	translation_ids = model.generate(**inputs)
	translation_text = tokenizer.batch_decode(translation_ids, skip_special_tokens=True)[0]
	print(datetime.datetime.now())
	return translation_text

def split_string_into_chunks(input_string, chunk_size):
    return [input_string[i:i+chunk_size + 25] for i in range(0, len(input_string), chunk_size)]


def read_file(file_path, chunk_size: int = 1500):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    return split_string_into_chunks(content, chunk_size)


def translate_file(input, output):
	a = read_file(input)

	tr = [trans(ch) for ch in a]

	with open(output, 'w') as file:
	    for line in tr:
			file.write(line + '\n')

args = sys.argv[1:]

translate_file(args[0], args[1])





