#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 27 12:57:01 2018

@author: kkmattil, 

This tool imports data to molecules and vials tables.

"""

# -*- coding: utf-8 -*-
import os
import pandas as pd
import mysql.connector as mysqlc
import openbabel as ob
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-i", "--input", dest="infilename",
                    help="csv formatted iput file", metavar="FILE", default="/home/kkmattil/Documents/DDCB/uef-data.csv")
parser.add_argument("-s", "--separator", dest="col_sep", default=";",
                    help="cloumn separator in the input file", metavar="SEP")
parser.add_argument("-c", "--mysqlconf", dest="mysql_conf", default="/home/kkmattil/Documents/DDCB/mysql_write.conf",
                    help="MySQL_conf", metavar="MYSQL_CONF")
parser.add_argument("-n", "--person", dest="person", default="joku_tumpelo",
                    help="Contact person for vial", metavar="db_host")
parser.add_argument("-l", "--department", dest="department", default="puuttuu",
                    help="Home department for vial", metavar="db_host")
parser.add_argument("-y", "--institute", dest="institute", default="puuttuu",
                    help="Home insititure for vial", metavar="db_host")
args = parser.parse_args()


print("Processing file:", args.infilename)
#define a short name for OpenBabel conversion tool
os.system("echo 'Vial_Barcode;External_ID;InChI_Code;Molecular_Weight;Purity;Method;Description;Vial_empty;Vial_total;Mass;Target_mass_min;Target_mass_max;n;V;' > vial_data_to_be_added.tmp  ")
grep_command=("grep InChI= " + str(args.infilename) + ">>vial_data_to_be_added.tmp" )

print(grep_command)
os.system( grep_command)

obConversion = ob.OBConversion()

#read in the data file
#vialdata =  pd.read_table('/home/kkmattil/Documents/DDCB/uef-data-only.csv', sep=";")
#vialdata =  pd.read_table(args.infilename, sep=args.col_sep)
vialdata =  pd.read_table("vial_data_to_be_added.tmp", sep=args.col_sep)

#Open mysql connection
cnx = mysqlc.connect(option_files=args.mysql_conf)
#cnx = mysqlc.connect(user=args.db_username, password=args.db_passwd, host=args.db_host, database=args.db_name)
cursor = cnx.cursor()

#Vials table               
create_table_if_needed = ('CREATE TABLE IF NOT EXISTS vials (vial_num INT AUTO_INCREMENT, vial_barcode VARCHAR(7), comp_num INT, external_ID VARCHAR(50), mol_weight DECIMAL(6,3), purity TINYINT, method VARCHAR(100), vial_empty DECIMAL(8,5), vial_tot DECIMAL(8,5), content_mass DECIMAL(8,5), target_mass_min DECIMAL(4,2), target_mass_max DECIMAL(4,2), description VARCHAR(1000), source_file VARCHAR(100), institute VARCHAR(100), department VARCHAR(100), person VARCHAR(100), date DATE, PRIMARY KEY (vial_num))  ENGINE=INNODB;')           
cursor.execute(create_table_if_needed)
cnx.commit()
cursor = cnx.cursor()

#Molecules table
create_table_if_needed = ('CREATE TABLE IF NOT EXISTS molecules ( comp_num INT AUTO_INCREMENT, inchi_code VARCHAR(4000), inchikey_code VARCHAR(27), smiles VARCHAR(4000), date DATE, PRIMARY KEY (comp_num))  ENGINE=INNODB;')
cursor.execute(create_table_if_needed)
cnx.commit()
cursor = cnx.cursor()

#query = ('SELECT vial_num FROM vials WHERE method = %s ;')
#SQL_query = ('SELECT comp_num FROM molecules WHERE inchi_code > %s ;')

##this is just a test
#a=(1)
#print(a)
#cursor.execute(query, (a, ))
#cursor.fetchall()  
#for (comp_num) in cursor:
#    print(comp_num)

#add_vial = ('INSERT INTO vials ( vial_barcode, comp_num, external_ID, mol_weight, Purity, method, vial_empty, vial_tot, content_mass, target_mass_min, target_mass_max ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ;')
SQL_add_vial = ('INSERT INTO vials ( vial_barcode, comp_num) VALUES (%s, %s) ;')
SQL_add_compound = ('INSERT INTO molecules ( inchi_code, smiles, inchikey_code ) VALUES (%s, %s, %s) ;')
#data_vial = ("NXNR")
#cursor.execute(add_vial, (data_vial, ))
#cnx.commit()
print(vialdata.shape[0])
nrows=vialdata.shape[0]

#go through the data farme row by row
for rown in range(0, nrows):
    inchi_data=vialdata.at[rown,"InChI_Code"]

    #stop the loop if InChI code is not defined
    if pd.isnull(inchi_data):
        break
    
    #First asign molecule number
    SQL_query = ('SELECT comp_num FROM molecules WHERE inchi_code = %s ;')
    cursor.execute(SQL_query, (inchi_data, ))
    print("inchi:", inchi_data)
    cursor.fetchall()
    #print("row count", cursor.rowcount)
    #print("SMILES:", smiles_data)
    
    # if code i snot fount in the moleclues table, 
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

    #Next fill in the vial data
    #Check the vial barcode format
    barcode=vialdata.at[rown,"Vial_Barcode"]
    if type(barcode) != "str":
        barcode_alku="FI-000"
        if barcode > 9:
            barcode_alku="FI-00"
        if barcode > 99:
            barcode_alku="FI-0"
        if barcode > 999:
            barcode_alku="FI-"
        vial_barcode=(barcode_alku + str(barcode))
    else:
        vial_barcode=barcode
    # set vial row
    vial_datarow = { "vial_barcode" : vial_barcode,
                     "comp_num": mol_id,
                     "external_ID" : vialdata.at[rown,"External_ID"],
                     "mol_weight" : vialdata.at[rown,"Molecular_Weight"],
                     "purity" : vialdata.at[rown,"Purity"],
                     "method" : vialdata.at[rown,"Method"],
                     "vial_empty" : vialdata.at[rown,"Vial_empty"],
                     "vial_tot" : vialdata.at[rown,"Vial_total"],
                     "content_mass" : vialdata.at[rown,"Mass"],
                     "target_mass_min" : vialdata.at[rown,"Target_mass_min"],
                     "target_mass_max" : vialdata.at[rown,"Target_mass_max"],
                     "description":  vialdata.at[rown,"Description"],
                     "source_file": args.infilename,
                     "institute": args.institute,
                     "department": args.department,
                     "person": args.person                     
                    }
#    vial_barcode=int(vialdata.at[rown,"Vial_Barcode"])  
#    external_ID=vialdata.at[rown,"External_ID"]
#    mol_weight=vialdata.at[rown,"Molecular_Weight"]
#    purity=vialdata.at[rown,"Purity"]
#    method=vialdata.at[rown,"Method"]
#    vial_empty=vialdata.at[rown,"Vial_empty"]
#    vial_tot=vialdata.at[rown,"Vial_total"]
#    content_mass=vialdata.at[rown,"Mass"]
#    target_mass_min=vialdata.at[rown,"Target_mass_min"]
#    target_mass_max=vialdata.at[rown,"Target_mass_max"]
    #date     

    #Check if vial barcode is already in use
    SQL_query = ('SELECT vial_barcode FROM vials WHERE vial_barcode = %s ;')
    cursor.execute(SQL_query, (vial_barcode, ))
    print("barcode:", vial_barcode)
    cursor.fetchall()
    

    if cursor.rowcount == 0:
         #cursor.execute(add_vial, (vial_barcode, mol_id, external_ID, mol_weight, purity, method, vial_empty, vial_tot, content_mass, target_mass_min, target_mass_max ))
         cursor.execute(SQL_add_vial, (vial_barcode, mol_id))
         cnx.commit()
         cursor = cnx.cursor()
    else:
        print("Vial Barcode already regitsred for:", vial_barcode)
        print("Updating the vial data:")
    
    vial_columns = ["comp_num", "external_ID", "mol_weight", "purity", "method", "vial_empty", "vial_tot", "content_mass", "target_mass_min", "target_mass_max"]
    vial_int_columns = ["comp_num","purity"]
    vial_float_columns = [ "mol_weight", "purity", "vial_empty", "vial_tot", "content_mass", "target_mass_min", "target_mass_max"]
    vial_string_columns = ["external_ID", "method", "description", "source_file", "institute", "department", "person"]
    #print(vial_columns[0])
    for vial_column in vial_int_columns:
          #print( vial_column, "=", vial_datarow[vial_column] )
          if pd.notnull(vial_datarow[vial_column]):
             new_value= int(vial_datarow[vial_column])
             print("Updating", vial_column, "=", new_value,"for vial:", vial_barcode)
             SQL_update_value = ('UPDATE vials SET ' + vial_column + ' = %s WHERE vial_barcode = %s ;' )
             #print(update_value, new_value, vial_barcode)
             cursor.execute(SQL_update_value, (new_value, vial_barcode))
             cnx.commit()
             cursor = cnx.cursor()
    for vial_column in vial_float_columns:
          #print( vial_column, "=", vial_datarow[vial_column] )
          if pd.notnull(vial_datarow[vial_column]):
             new_value= float(str(vial_datarow[vial_column]).replace(',','.'))
             print("Updating", vial_column, "=", new_value, "for vial:", vial_barcode)
             SQL_update_value = ('UPDATE vials SET ' + vial_column + ' = %s WHERE vial_barcode = %s ;' )
             cursor.execute(SQL_update_value, (new_value, vial_barcode))
             cnx.commit()
             cursor = cnx.cursor()  
    for vial_column in vial_string_columns:
          #print( vial_column, "=", vial_datarow[vial_column] )
          if pd.notnull(vial_datarow[vial_column]):
             new_value= str(vial_datarow[vial_column])
             new_value=new_value.strip("'")
             print("Updating", vial_column, "=", new_value, "for vial:", vial_barcode)
             SQL_update_value = ('UPDATE vials SET ' + vial_column + ' = "%s" WHERE vial_barcode = %s ;' )
             #print(update_value, new_value, vial_barcode)
             cursor.execute(SQL_update_value, (new_value, vial_barcode))
             cnx.commit()
             cursor = cnx.cursor()  
     
    SQL_add_date=("UPDATE vials SET date = CURRENT_DATE WHERE vial_barcode = %s ");
    cursor.execute(SQL_add_date, (vial_barcode, ))
    cnx.commit()
    cursor = cnx.cursor()   
 #             "target_mass_max" : float(vialdata.at[rown,"Target_mass_max"].replace(',','.')) vial_barcode, comp_num, external_ID, mol_weight, Purity, method, vial_empty, vial_tot, content_mass, target_mass_min, target_mass_max
    
    

cnx.commit()
cnx.close()
os.system("rm -rf vial_data_to_be_added.tmp" )