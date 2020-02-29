import simplejson as json
from cassandra.cluster import *
import re
import sys
from tqdm import tqdm


def get_product_sql(content):
	sql = "INSERT INTO PRODUCT "
	columns = ['id', 'title', 'ASIN', 'group', 'salesrank']
	columns_to_insert = ''
	values_to_insert = ''
	for column in columns:
		if(column in content):
			columns_to_insert += ', ' + column
			if(column != 'id' and column != 'salesrank'):
				if(column == 'title'):
					values_to_insert += ", '" + content[column].replace("'", "''") + "'"
				else:
					values_to_insert += ", '" + content[column] + "'"
			else:
				values_to_insert += ', ' + str(content[column])
	sql = sql + '(' + columns_to_insert[1:] + ') VALUES (' + values_to_insert[1:] + ')'
	return sql.replace('group', 'group')

def get_product_category_sql(content):
	if('categories' not in content):
		return []
	commands = []
	for category_hierarchy in content['categories']:
		category = re.search('\d+\]$', category_hierarchy)
		if(category != None):
			category = category.group()[:-1]
			sql = "INSERT INTO PRODUCT_CATEGORY (productid, categoryid) VALUES ( " + content['id'] +", " + category +")"
			commands.append(sql)
	return commands

customer_ids = {}

def get_customer_sql(content):
	if('reviews' not in content):
		return []
	reviews = content['reviews']
	sql_commands = []
	for review in reviews:
		if(review['customerid'] in customer_ids):
			continue
		customer_id = str(review['customerid'])

		customer_ids[customer_id] = 1
		sql = "INSERT INTO CUSTOMER (id) VALUES ('" + customer_id +"')"
		sql_commands.append(sql)
	return sql_commands

def get_product_similar_sql(content):
	if('similar_items' not in content):
		return []
	similar_items = content['similaritems']
	sql_commands = []
	for  similar in similar_items:
		sql = "INSERT INTO PRODUCT_SIMILAR (similarasin, product_id) VALUES ('" + similar +"', '" + content['id'] + "')"
		sql_commands.append(sql)
	return sql_commands

def get_reviews_sql(content):
	if('reviews' not in content):
		return []
	reviews = content['reviews']
	sql_commands = []
	columns = ['rating', 'votes', 'data', 'helpful', 'customerid']
	
	for review in reviews:
		columns_to_insert = ''
		values_to_insert = ''
		for column in columns:
			if(column in review):
				columns_to_insert += ', ' + column
				if(column == 'data' or column == 'customerid'):
					values_to_insert += ", '" + str(review[column]) + "'"
				else:
					values_to_insert += ', ' + str(review[column])
		sql = "INSERT INTO REVIEW ( productid " + columns_to_insert + ") VALUES (" + content['id'] + values_to_insert + ")"
		sql_commands.append(sql)
	return sql_commands

category_ids = {}

def get_category_sql(content):
	if('categories' not in content):
		return []
	all_categories = content['categories']
	commands = []
	for category_hierarchy in all_categories:
		categories = category_hierarchy.split("|")[1:]
		for i in range(0, len(categories)):
			temp = categories[i].split("[")
			category_id = temp[-1][:-1]
			if(category_id in category_ids):
				continue
			category_ids[category_id] = 1
			name = temp[0]
			if(i == 0):
				parent_id = "NULL"
			else:
				parent_id = categories[i - 1].split("[")[1][:-1]
			sql = "INSERT INTO CATEGORY (id, name, parentid) VALUES ("+ category_id +", '" + name.replace("'", r"''") + "', " + parent_id +" )"
			commands.append(sql)
	return commands


def generate_inserts():
	file_path = "amazon.json"
	with open(file_path) as f:
		lines = f.readlines()
		for line in tqdm(lines[1:], total=len(lines)-1):
			content = json.loads(line)
			
			yield get_product_sql(content)

			for command in get_category_sql(content):
				yield command

			for command in get_customer_sql(content):
				yield command

			for command in get_reviews_sql(content):
				yield command

			for command in get_product_category_sql(content):
				yield command

			for command in get_product_similar_sql(content):
				yield command

if __name__ == '__main__':
	
	cluster = Cluster()
	session = cluster.connect()
	
	session.execute("CREATE KEYSPACE IF NOT EXISTS amazon WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '3' }")
	
	session.set_keyspace("amazon")
	
	session.execute("DROP TABLE IF EXISTS PRODUCT")
	session.execute("CREATE TABLE PRODUCT (title text, id int, ASIN text, group text, salesrank int, PRIMARY KEY (id))")
	
	session.execute("DROP TABLE IF EXISTS CUSTOMER")
	session.execute("CREATE TABLE CUSTOMER(id text, PRIMARY KEY (id))")

	session.execute("DROP TABLE IF EXISTS PRODUCT_SIMILAR ")
	session.execute("CREATE TABLE PRODUCT_SIMILAR(similarasin text, productid int , PRIMARY KEY (similarasin, productid))")

	session.execute("DROP TABLE IF EXISTS  REVIEW")
	session.execute("CREATE TABLE REVIEW(rating int,votes int, data date, helpful int, productid int, customerid text, PRIMARY KEY (productid, customerid))")

	session.execute("DROP TABLE IF EXISTS  CATEGORY")
	session.execute("CREATE TABLE CATEGORY(id INT,name text,parentid INT,PRIMARY KEY (id))")

	session.execute("DROP TABLE IF EXISTS PRODUCT_CATEGORY")
	session.execute("CREATE TABLE PRODUCT_CATEGORY(productid INT,categoryid INT,PRIMARY KEY (productid, categoryid))")
	for command in generate_inserts():
				if(command):
					session.execute(command)