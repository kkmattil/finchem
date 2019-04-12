#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 22 11:36:28 2019

@author: kkmattil
"""
from argparse import ArgumentParser
import mysql.connector as mysqlc
from bioservices import UniChem


#Define command line options and defaults
parser = ArgumentParser()
parser.add_argument("-c", "--mysqlconf", dest="mysql_conf", default="/home/kkmattil/Documents/DDCB/mysql_write.conf",
                    help="MySQL_conf", metavar="MYSQL_CONF")
args = parser.parse_args()


uniC = UniChem()


#Open mysql connection
cnx = mysqlc.connect(option_files=args.mysql_conf)

cursor = cnx.cursor()

SQL_drop_table=('DROP TABLE unichem_links;')
cursor.execute(SQL_drop_table)
cnx.commit()
cursor = cnx.cursor()


SQL_create_table_if_needed = ('CREATE TABLE IF NOT EXISTS unichem_links(comp_num INT,  source_id INT, source VARCHAR(200), id_in_db  VARCHAR(200)) ENGINE=INNODB;')
cursor.execute(SQL_create_table_if_needed)
cnx.commit()
cursor = cnx.cursor()
#unichem_dbs=uniC.source_names
#the above command produces outdated list

unichem_dbs = {
            1:"chembl",
            2:"drugbank",
            3:"pdb",
            4:"gtopdb",
            5:"pubchem_dotf",
            6:"kegg_ligand",
            7:"chebi",
            8:"nih_ncc",
            9:"zinc",
            10:"emolecules",
            11:"ibm",
            12:"atlas",
            14:"fdasrs",
            15:"surechembl",
            17:"pharmgkb",
            18:"hmdb",
            20:"selleck",
            21:"pubchem_tpharma",
            22:"pubchem",
            23:"mcule",
            24:"nmrshiftdb2",
            25:"lincs",
            26:"actor",
            27:"recon",
            28:"molport",
            29:"nikkaji",
            31:"bindingdb",
            32:"comptox",
            33:"lipidmaps",
            34:"drugcentral",
            35:"carotenoiddb",
            36:"metabolights",
            37:"brenda",
            38:"rhea",
            39:"chemicalbook"
            }



#inchikey=("LTDKIGOMEHDLGI-UHFFFAOYSA-N")

SQL_query = ('SELECT comp_num, inchikey_code FROM molecules')
SQL_add_links = ('INSERT INTO unichem_links ( comp_num, source_id, source, id_in_db ) VALUES (%s, %s, %s, %s) ;')
cursor.execute(SQL_query)
#print("inchi:", inchi_data)
mlist=cursor.fetchall()

cnx.commit()
cursor = cnx.cursor()

for comp_num, inchikey in mlist:    
    uc_ids=uniC.get_src_compound_ids_all_from_inchikey(inchikey)
    if uc_ids == 404:
        print("Unichem enytry for:", comp_num, inchikey, " not found" )
    else:    
      for uc_hit in uc_ids:
           dbid=int(uc_hit["src_id"])
           #dbid=uc_hit["src_id"]
           dbname=unichem_dbs[int(dbid)]
           print(comp_num, inchikey, dbname, dbid, uc_hit["src_compound_id"])
           cursor.execute(SQL_add_links, (comp_num, dbid, dbname, uc_hit["src_compound_id"] ))
           cnx.commit()
           cursor = cnx.cursor()