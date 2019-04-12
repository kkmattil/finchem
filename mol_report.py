#!/opt/chipster/tools_local/miniconda3/bin/python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 25 17:57:29 2019

@author: kkmattil
"""

import pybel 
import mysql.connector as mysqlc
from argparse import ArgumentParser
import requests
import json
import base64

#Define command line options and defaults
parser = ArgumentParser()
parser.add_argument("-c", "--mysqlconf", dest="mysql_conf", default="/opt/chipster/tools_local/finscreen/mysql_read.conf",
                    help="MySQL_conf", metavar="MYSQL_CONF")
parser.add_argument("-i", "--inchi", dest="inchi", default="x",
                    help="InChI", metavar="INCHI")
parser.add_argument("-k", "--inchi_key", dest="inchi_key", default="x",
                    help="InChI key", metavar="INCHI_KEY")
parser.add_argument("-s", "--smiles", dest="smiles", default="x",
                    help="Smiles", metavar="SMILES")
args = parser.parse_args()

#ickey=("NLAHRBKSSHWXQT-UHFFFAOYSA-N")
#Define the database connection
#cnx = mysqlc.connect(user=args.db_username, password=args.db_passwd, host=args.db_host, database=args.db_name)
#cnx = mysqlc.connect(user='dbuser', password='dbpass', host='195.148.30.95', database='finscreen')
cnx = mysqlc.connect(option_files=args.mysql_conf)
#cnx = mysqlc.connect(option_files="/opt/chipster/tools_local/finscreen/mysql_read.conf")
cursor = cnx.cursor()

keynum=0 

#Check the query term
if args.inchi_key != "x":
    SQL_query = ('SELECT comp_num, inchi_code, inchikey_code, smiles FROM molecules WHERE inchikey_code = %s ;')
    key=args.inchi_key
    keynum=keynum+1

if args.inchi != "x":
    SQL_query = ('SELECT comp_num, inchi_code, inchikey_code, smiles FROM molecules WHERE inchi_code = %s ;')
    key=args.inchi
    keynum=keynum+1

if args.smiles != "x":
    SQL_query = ('SELECT comp_num, inchi_code, inchikey_code, smiles FROM molecules WHERE inchi_code = %s ;')
    key=args.smile
    keynum=keynum+1
if keynum == 1 :
   print(key)
else:
   print("define one and only one molecule identifier")
   exit()
#SQL_query = ('SELECT comp_num, inchi_code, inchikey_code, smiles FROM molecules WHERE inchikey_code = %s ;')
cursor.execute(SQL_query,(key,) )
molecule=cursor.fetchone()

f=open("report.html","w+")
f.write("<html>\n")
f.write("<body>\n")


if cursor.rowcount == 0:
   f.write("<b>Error: The defined molecule is not found:</b><br>\n")
   f.write(key)
   f.write("</body>\n")
   f.write("</html>\n")
   exit()
   
cursor = cnx.cursor()

comp_num=molecule[0]
mol=pybel.readstring("inchi", molecule[1])
SQL_query = ('SELECT formula, mol_weight_ob FROM molecule_data WHERE comp_num = %s ;')
cursor.execute(SQL_query,(comp_num,) )
datarow=cursor.fetchone()
cursor = cnx.cursor()

f=open("report.html","w+")
f.write("<html>\n")
f.write("<body>\n")
f.write("<h1>Molecule report</h1>\n")
f.write("<h2>Molecule identifiers</h2>\n")
f.write("<table border=1>\n")
li=("<tr><td>InChI:</td><td>" + molecule[1] + "</td</tr><tr><td>InChI_key:</td><td>" + molecule[2] + "</td></tr><tr><td>SMILES:</td><td> " + molecule[3] + "</td></tr>\n")
print(li)

f.write("%s\r\n" % li)
f.write("</table>\n")
f.write("<h2>General data</h2>\n")
li2=("<tr><td>Formula:</td><td>" + datarow[0] + "</td></tr><tr><td>Computed molecular weight:</td><td>" + str(datarow[1]) + "</td></tr>")
f.write("<table border=1>\n")
f.write("%s\r\n" % li2)
mol.draw(show=False, filename="mol_structure.png")
#convert ímage to string
pngdata = base64.b64encode(open("mol_structure.png",'rb').read())
dpngdata = pngdata.decode()
f.write('<tr><td>Structure:</td><td><img src="data:image/png;base64,%s" alt="molecule structure"></td></tr>' % dpngdata)
#f.write('<tr><td>Structure:</td><td><img src="mol_structure.png" alt="molecule structure"></td></tr>')
f.write("</table>\n")
f.write('<h2>Vials</h2>\n')


#Vial data
SQL_query = ('SELECT vial_barcode, external_ID, mol_weight, purity, method, vial_empty, vial_tot, content_mass, target_mass_min, target_mass_max, description FROM vials WHERE comp_num = %s ;')
cursor.execute(SQL_query,(comp_num,) )
vial_list=cursor.fetchall()
f.write("<table border=1>\n")
f.write('<tr><td>vial_barcode</td><td>external_ID</td><td>mol_weight</td><td>purity</td><td>method</td><td>vial_empty</td><td>vial_tot</td><td>content_mass</td><td>target_mass_min</td><td>target_mass_max</td><td>description</td></tr>\n')

for (vial_barcode, external_ID, mol_weight, purity, method, vial_empty, vial_tot, content_mass, target_mass_min, target_mass_max, description) in vial_list:
  #print(inchikey, comp_num)
     vial_li=("<tr><td>" + str(vial_barcode) + "</td><td>" + str(external_ID).strip("'") + "</td><td>" + str(mol_weight) + "</td><td>" + str(purity) + "</td><td>" + str(method) + "</td><td>" + str(vial_empty) + "</td><td>"+ str(vial_tot) + "</td><td>" + str(content_mass) + "</td><td>" + str(target_mass_min) + "</td><td>" + str(target_mass_max) + "</td><td>" + str(description) + "</td></tr>\n")
     f.write(vial_li)
f.write("</table>\n")

f.write('<h2>External links</h2>\n')

#Look for the links in link table
SQL_query = ('SELECT source_id, source, id_in_db  FROM unichem_links WHERE comp_num = %s ;')
cursor.execute(SQL_query,(comp_num,) )
unichem_list=cursor.fetchall()
f.write("<table border=1>\n")
for (source_id, source, id_in_db ) in unichem_list:
    #li3=("<tr><td>" + source + "</td><td>" + id_in_db)
    api_url=("https://www.ebi.ac.uk/unichem/rest/src_compound_id_url/"+ str(id_in_db)+"/"+str(source_id)+"/"+str(source_id))
    print(api_url)
    myResponse = requests.get(api_url) 
    db_link=("not available")
    li3=("<tr><td>" + source + "</td><td>" + id_in_db + "</td>")
    if(myResponse.ok):
        jData = json.loads(myResponse.content)
        if len(jData) > 0 :
           a=jData[0]
           db_link=a["url"]
           li3=(li3 +"<td><a href="+db_link+">Link</a></td></tr>\n")
        else:
           li3=(li3 +"<td>No direct link</td></tr>\n")   
    f.write(li3)
f.write("</table>\n")
f.write("</body>\n")

f.write("</html>\n")

f.close
