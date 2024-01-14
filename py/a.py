import torch

from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from transformers import MarianMTModel, MarianTokenizer
import datetime
import sys
import os

args = sys.argv[1:]

compute_type = args[0]
input_file = args[1]
partition = int(args[2])
partition_total = int(args[3])


path_prefix = '/scratch/project_2006264/diss/ua'
output_path_prefix = '/scratch/project_2006264/diss/ru'

#tokenizer = AutoTokenizer.from_pretrained("Helsinki-NLP/opus-mt-uk-ru")
#model = AutoModelForSeq2SeqLM.from_pretrained("Helsinki-NLP/opus-mt-uk-ru").to('cuda')

model_name = "Helsinki-NLP/opus-mt-uk-ru"

if compute_type == 'cuda':
    model = MarianMTModel.from_pretrained(model_name).to('cuda')
else:
    model = MarianMTModel.from_pretrained(model_name)

tokenizer = MarianTokenizer.from_pretrained(model_name)

def get_line(f):
     with open(f) as file:
         for i in file:
             yield i

def translate(text):
    input_ids = tokenizer(text, return_tensors="pt", truncation=True).input_ids.to(model.device)
    print(datetime.datetime.now())    
    with torch.no_grad():
        outputs = model.generate(input_ids=input_ids)
    
    return tokenizer.decode(outputs[0], skip_special_tokens=True)


def trans(text):
	inputs = tokenizer(text, return_tensors="pt", truncation=True)
	translation_ids = model.generate(**inputs)
	translation_text = tokenizer.batch_decode(translation_ids, skip_special_tokens=True)[0]
	print(datetime.datetime.now())
	return translation_text

def split_string_into_chunks(input_string, chunk_size):
    return [input_string[i:i+chunk_size + 25] for i in range(0, len(input_string), chunk_size)]


def read_file(file_path, chunk_size: int = 1500):
    try: 
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        return split_string_into_chunks(content, chunk_size)
    except UnicodeDecodeError:
        print("UnicodeDecodeError at {}".format(file_path))
        return []


def translate_file(input, output):
	a = read_file(input)
	tr = [translate(ch) for ch in a]
	with open(output, 'w') as file:
            for line in tr:
                file.write(line + '\n')

def main():
    print("main started")
    for line in get_line(input_file):
        split = line.strip().split("\t")
        p = split[0][4:]
        dir_id = int(p.split("/")[0])
        if dir_id % partition_total == partition:
            input_path = path_prefix + '/' + p
            output_path = output_path_prefix + '/' + p
            output_path_dir = os.path.dirname(output_path)
            lang = split[1]
            # print("lang={}".format(lang))
            if os.path.isfile(input_path) and not (os.path.isfile(output_path)) and (lang.endswith("_uk") or os.path.getsize(input_path) <= 16000):
                print("translate {} to {}".format(input_path, output_path))
                os.makedirs(output_path_dir, exist_ok=True)
                translate_file(input_path, output_path)

main()
