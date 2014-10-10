#!/usr/bin/python

# Makes sql insert statements for a particular table.
# Configuration of this script means changing its source.
# YMMV

import re
import random


# flags
dict_filename = "/usr/share/dict/words"
first_id_inclusive = 0
last_id_exclusive = 4
db_name = "sql-serv"
table_name = "Medium_Size"
print_use_db_line = True


# note: ');' line  separator used by c.g.e.a.database.InsertSQL program
RECORD_FORMAT = """
insert into %s(part, description, seller_id, seller_name, price, notes, stock
) VALUES ( "%s", "%s", %d, "%s", %0.2f, "%s", %d 
);"""


# reference, on how table being inserted into, was made
"""
create table thousand (
  part varchar(48),
  description varchar(2024),
  seller_id int,
  seller_name varchar(128),
  price decimal,
  notes varchar(7168),
  stock int,
  id int not null auto_increment primary key
);
"""


# data structure
words = []
word_index = 0


def init_data_structure():
  dict_file = open(dict_filename)
  for line in dict_file:
    line = re.sub("[^0-9a-zA-Z]", "", line)
    line = line.strip()
    if len(line) > 0:
      words.append(line)
  dict_file.close()
  random.shuffle(words)


def next_word():
  global word_index
  word = words[word_index]
  word_index = word_index + 1
  word_index = word_index % (len(words))
  if 0 == word_index:
    random.shuffle(words)
  return word


def make_vchar(max_len):
  chosen = []
  used = 0
  while used < max_len:
    word = next_word()
    chosen.append(word)
    used = used + len(word) + 1
  vchar = ' '.join(chosen)
  return vchar[:max_len]


def main():
  init_data_structure()

  if print_use_db_line:
    print "use %s;" % db_name
  
  i = first_id_inclusive
  while i < last_id_exclusive:
    part = make_vchar(48)
    des = make_vchar(2024)
    seller_id = i
    seller_name = make_vchar(128)
    price = float(i)
    notes = make_vchar(7168)
    stock = (last_id_exclusive * 5) - i
    record = RECORD_FORMAT % (table_name, part, des, seller_id,
        seller_name, price, notes, stock)
    print record
    i = i + 1


main()
