#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 25 13:09:25 2019

@author: kkmattil
"""

# -*- coding: utf-8 -*-
from __future__ import print_function
from rdkit import Chem
from rdkit.Chem import AllChem
from argparse import ArgumentParser
from rdkit import DataStructs
from rdkit.Chem.Fingerprints import FingerprintMols
import mysql.connector as mysqlc

#Define command line options and defaults
parser = ArgumentParser()
parser.add_argument("-i", "--inputfile", dest="input_data", default="finchem_kanoninen.sd",
                    help="query file", metavar="INPUT")
parser.add_argument("-r", "--reference", dest="reference_data", default="/home/kkmattil/Downloads/chembl_25.sdf",
                    help="reference file", metavar="INPUT")
parser.add_argument("-o", "--outputfile", dest="output_data", default="molecule.report",
                    help="output file", metavar="OUTPUT")
parser.add_argument("-t", "--thereshold", dest="threshold", default=0.6,
                    help="output file", metavar="OUTPUT")
parser.add_argument("-n", "--inputformat", dest="inputformat", default="sdf",
                    help="input format", metavar="INPUT")
parser.add_argument("-f", "--format", dest="mol_format", default="mol2",
                    help="Input Format", metavar="FORMAT")
parser.add_argument("-c", "--mysqlconf", dest="mysql_conf", default="/home/kkmattil/Documents/DDCB/mysql_write.conf",
                    help="MySQL_conf_finscreen", metavar="MYSQL_CONF")
args = parser.parse_args()


#Define the database connection
cnx = mysqlc.connect(option_files=args.mysql_conf)
cursor = cnx.cursor()

#Create table
SQL_create_table_if_needed = ('CREATE TABLE IF NOT EXISTS chembl_similarity(comp_name VARCHAR(20), chembl_name VARCHAR(20), similarity DECIMAL(18,17)) ENGINE=INNODB;')
cursor.execute(SQL_create_table_if_needed)
cnx.commit()
cursor = cnx.cursor()

#Convert refernce inchi to into sdf file

if args.inputformat == "sfd":
   w = Chem.SDWriter('foo.sdf')
   inchi_infile = open(args.input_data,  'r')
   for line in inchi_infile:
       print(line.strip())
       mol=Chem.MolFromInchi(line.strip())
       w.write(mol)


#fps_query=dict()

query_set = Chem.SDMolSupplier(args.input_data)
#for query_mol in query_set:
#    query_name=query_mol.GetProp("_Name")
#    fp_query=FingerprintMols.FingerprintMol(query_mol)
#    print(query_name, " ", fp_query)
#    fps_query.update(query_name=fp_query) 


a=0
b=1

#reference_set = Chem.SDMolSupplier(args.reference_data)
#for query_mol in query_set:
#    ref_name=query_mol.GetProp("_Name")
#    print(ref_name)
    
    
reference_set = Chem.SDMolSupplier(args.reference_data)
for ref_mol in reference_set:
    fp_ref=FingerprintMols.FingerprintMol(ref_mol)
    ref_name=ref_mol.GetProp("_Name")
    a=a+1
    b=b+1
    if b > 1000 :
        (print(a))
        b=0
    for query_mol in query_set:
       query_name=query_mol.GetProp("_Name")
       fp_query=FingerprintMols.FingerprintMol(query_mol)        
       simi=DataStructs.FingerprintSimilarity(fp_ref,fp_query)
       if simi > args.threshold:
           SQL_add_comp_num = ('INSERT INTO chembl_similarity ( comp_name, chembl_name, similarity ) VALUES (%s, %s, %s) ;')
           cursor.execute(SQL_add_comp_num, (query_name, ref_name, simi ))
           cnx.commit()
           cursor = cnx.cursor()
           
           print(query_name, " ", ref_name, " ", simi)
