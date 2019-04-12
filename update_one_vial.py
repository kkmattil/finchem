#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 11 14:46:52 2019

@author: kkmattil
"""

import os
import openbabel as ob
import mysql.connector as mysqlc
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-f", "--field", dest="column", default="method",
                    help="column name", metavar="FIELD")
parser.add_argument("-v", "--value", dest="value", default="testi",
                    help="Value", metavar="VALUE")
parser.add_argument("-b", "--barcode", dest="barcode", default="FI-0005",
                    help="Vial barcode", metavar="BARCODE")
parser.add_argument("-c", "--mysqlconf", dest="mysql_conf", default="/home/kkmattil/Documents/DDCB/mysql_write.conf",
                    help="MySQL_conf", metavar="MYSQL_CONF")
args = parser.parse_args()


#Open mysql connection
cnx = mysqlc.connect(option_files=args.mysql_conf)
cursor = cnx.cursor()

update_column=args.column
update_value=args.value

#asign molecule number if inchi_key is updated
if args.column == "inchi_code":
    inchi_data=args.value
    obConversion = ob.OBConversion()
    SQL_query = ('SELECT comp_num FROM molecules WHERE inchi_code = %s ;')
    SQL_add_compound = ('INSERT INTO molecules ( inchi_code, smiles, inchikey_code ) VALUES (%s, %s, %s) ;')
    cursor.execute(SQL_query, (args.value, ))
    cursor.fetchall()
    #print("row count", cursor.rowcount)
    #print("SMILES:", smiles_data)
    
    # if code is not fount in the moleclues table, 
    # add it to the table
    if cursor.rowcount == 0:
         #Define SMILES string and other information
         obConversion.SetInAndOutFormats("inchi", "smi")
         mol = ob.OBMol()
         obConversion.ReadString(mol, inchi_data)
         smiles_data = obConversion.WriteString(mol)
         smiles_data = smiles_data.strip()
        
         obConversion.SetInAndOutFormats("inchi", "inchikey")
         obConversion.ReadString(mol, inchi_data)
         inchikey_data = obConversion.WriteString(mol)
         inchikey_data = inchikey_data.strip()
         
         cursor = cnx.cursor()
         
         print("adding", smiles_data )
         cursor.execute(SQL_add_compound, (inchi_data, smiles_data, inchikey_data ))
         cnx.commit()
         cursor = cnx.cursor()
         #query = ('SELECT comp_num FROM molecules WHERE inchi_code = %s ;')
         cursor.execute(SQL_query, (inchi_data, ))
         idlist=cursor.fetchall()
         idrow=idlist[0]
         mol_id=idrow[0]         
         print("New molecule registered with index number:", mol_id)
         SQL_add_date=("UPDATE molecules SET date = CURRENT_DATE WHERE comp_num=%s");
         cursor.execute(SQL_add_date, (mol_id, ))
         cnx.commit()
         cursor = cnx.cursor()
    else:
         cursor.execute(SQL_query, (inchi_data, ))
         idlist=cursor.fetchall()
         idrow=idlist[0]
         mol_id=idrow[0]
         print("This molecule is already registered as: ", mol_id)
 
    update_column="comp_num"
    update_value=mol_id


#check if vial exists
SQL_query = ('SELECT COUNT(*) FROM vials WHERE vial_barcode = %s;')
cursor.execute(SQL_query, (args.barcode, ))
print("Update:", args.barcode, update_column, "is set to " ,update_value)

row = cursor.fetchone()
sqlcount=row[0]

if sqlcount == 0:
#New Vial
   SQL_add_vial = ('INSERT INTO vials ( vial_barcode,'+ update_column +', source_file ) VALUES (%s, %s, "%s") ;')
   cursor.execute(SQL_add_vial, (args.barcode, update_value, "manual editing" ))
   cnx.commit()
   cursor = cnx.cursor()
else:  
#Update existing
    SQL_update_vial = ('UPDATE vials SET ' + update_column + ' = %s WHERE vial_barcode = %s ;' )
    cursor.execute(SQL_update_vial, (update_value, args.barcode ))
    cnx.commit()
    cursor = cnx.cursor()
    SQL_update_vial = ('UPDATE vials SET source_file = "%s" WHERE vial_barcode = %s ;' )
    cursor.execute(SQL_update_vial, ("manual editing", args.barcode ))
    cnx.commit()
    cursor = cnx.cursor()

#update date    
SQL_add_date=("UPDATE vials SET date = CURRENT_DATE WHERE vial_barcode = %s ");
cursor.execute(SQL_add_date, (args.barcode, ))
cnx.commit()
cursor = cnx.cursor()   


cnx.close()
