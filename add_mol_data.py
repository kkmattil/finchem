#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 18 08:54:38 2019

@author: kkmattil
"""
#inchi_data=("InChI=1S/C14H18N4O2/c1-14(2,7-11(16)19)8-12-17-13(18-20-12)9-4-3-5-10(15)6-9/h3-6H,7-8,15H2,1-2H3,(H2,16,19)")
import pybel 
import mysql.connector as mysqlc
from argparse import ArgumentParser
#mol=pybel.readstring("inchi", inchi_data)


#Define command line options and defaults
parser = ArgumentParser()
parser.add_argument("-f", "--finchem", dest="finchem_conf", default="/home/kkmattil/Documents/DDCB/mysql_write.conf",
                    help="MySQL_conf_finscreen", metavar="MYSQL_CONF")
parser.add_argument("-c", "--chembl", dest="chembl_conf", default="/home/kkmattil/Documents/DDCB/mysql_write_chembl.conf",
                    help="MySQL_conf_chembl", metavar="MYSQL_CONF")
args = parser.parse_args()


#Define the database connection
cnx = mysqlc.connect(option_files=args.finchem_conf)
#cnx = mysqlc.connect(user='dbuser', password='dbpass', host='195.148.30.95', database='finscreen')
cursor = cnx.cursor()


#Define the chembl database connection
#che_cnx= mysqlc.connect(user='dbuser', password='dbpass', host='195.148.30.95', database='chembl_24')
che_cnx=mysqlc.connect(option_files=args.chembl_conf)
che_cursor = che_cnx.cursor()

SQL_create_table_if_needed = ('CREATE TABLE IF NOT EXISTS molecule_data(comp_num INT, formula VARCHAR(200), mol_weight_ob DECIMAL(6,3), filename VARCHAR(500), chembl_molregno BIGINT(20), chembl_id VARCHAR(20), data BLOB, date DATE, PRIMARY KEY (comp_num)) ENGINE=INNODB;')
cursor.execute(SQL_create_table_if_needed)
cnx.commit()
cursor = cnx.cursor()

SQL_query = ('SELECT comp_num, inchi_code, inchikey_code FROM molecules WHERE 1=1 ;')
SQL_update_mol_prop = ('UPDATE molecule_data SET formula = %s, mol_weight_ob = %s WHERE comp_num = %s ;' )

cursor.execute(SQL_query, )
comp_list=cursor.fetchall()

output = pybel.Outputfile("sdf", "finchem_molecules.sd")

for (comp_num, inchi, inchikey) in comp_list:
  #print(inchikey, comp_num)
  
    SQL_comp_num_query = ('SELECT comp_num FROM molecule_data WHERE comp_num = %s ;')
    cursor.execute(SQL_comp_num_query, (comp_num, ))
    cursor.fetchall()
    #print("row count", cursor.rowcount)
    #print("SMILES:", smiles_data)
    
    # if comp_num is not found in the moleclue_data table, 
    # add it to the table
    if cursor.rowcount == 0:
        SQL_add_comp_num = ('INSERT INTO molecule_data ( comp_num ) VALUES (%s) ;')
        cursor.execute(SQL_add_comp_num, (comp_num, ))
        cnx.commit()
        cursor = cnx.cursor()
   
    
    
    mol=pybel.readstring("inchi", inchi)
    mol.title=("finchem_" + str(comp_num))
    ickey=str.strip(inchikey)
    print(ickey)
    SQL_chembl_query=('SELECT molregno FROM compound_structures WHERE standard_inchi_key = %s;')
    che_cursor.execute(SQL_chembl_query,(ickey,) )
    regnolist=che_cursor.fetchall()


    # Use InChiKey to check if the compound is found in Chembl database
    if che_cursor.rowcount > 0:
       regnorow=regnolist[0]
       regno=regnorow[0]
       print("ChemBL regno:", regno)
       SQL_chembl_query=('SELECT chembl_id FROM molecule_dictionary WHERE molregno = %s;')
       che_cursor.execute(SQL_chembl_query,(regno,) )
       chembl_id_list=che_cursor.fetchall()
       chembl_id_row=chembl_id_list[0]
       chembl_id=chembl_id_row[0]  
       print("ChEMBL regno:", regno, " Chembl_ID:", chembl_id)
       SQL_update_value = ('UPDATE molecule_data SET chembl_molregno = %s WHERE comp_num = %s ;' )
       #print(update_value, new_value, vial_barcode)
       cursor.execute(SQL_update_value, (regno, comp_num))
       cnx.commit()
       cursor = cnx.cursor()
       SQL_update_value = ('UPDATE molecule_data SET chembl_id = %s WHERE comp_num = %s ;' )
       #print(update_value, new_value, vial_barcode)
       cursor.execute(SQL_update_value, (chembl_id, comp_num))
       cnx.commit()
       cursor = cnx.cursor()
       

    else:
       print("InChI_key", ickey, "does not have mactch in Chembl" )
       
       
    #Add other data

    mol.make3D()
    
    SQL_update_value = ('UPDATE molecule_data SET formula = %s , mol_weight_ob = %s, data = %s, date = CURRENT_DATE WHERE comp_num = %s ;' )
    cursor.execute(SQL_update_value, (mol.formula, mol.molwt, mol.write("sdf"), comp_num))
    cnx.commit()
    cursor = cnx.cursor()
     
    output.write(mol)

output.close()



#    #Add other data
#    SQL_update_value = ('UPDATE molecule_data SET mol_weight_ob = %s WHERE comp_num = %s ;' )
#    cursor.execute(SQL_update_value, (mol.molwt, comp_num))
#    cnx.commit()
#    cursor = cnx.cursor()
    
    #print(mol.molwt)
#    mol.make3D()
#    SQL_update_value = ('UPDATE molecule_data SET data = %s WHERE comp_num = %s ;' )
#    cursor.execute(SQL_update_value, (mol.write("sdf"), comp_num))
#    cnx.commit()
#    cursor = cnx.cursor()
    
#    SQL_add_date=("UPDATE molecule_data SET date = CURRENT_DATE WHERE comp_num = %s ");
#    cursor.execute(SQL_add_date, (comp_num, ))
#    cnx.commit()
#    cursor = cnx.cursor()   
    
    #print(mol.write("sdf"))