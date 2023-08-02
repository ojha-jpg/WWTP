
import mysql.connector


connection = mysql.connector.connect(
    host='127.0.0.1',
    port ='3306',
    user='root',
    password='Kathmandu2345',
    database='ashish'
    )

# Create a cursor object to execute SQL commands.
cursor = connection.cursor()
    
# delete table     
drop_table_query = "DROP TABLE IF EXISTS final_data"

try:
    # Execute the SQL query using the cursor.
    cursor.execute(drop_table_query)
    print("Table 'final_data' deleted successfully!")
except mysql.connector.Error as e:
    print(f"Error: {e}")
    

#create table
create_table_query = '''
    create table final_data
(
    date_ DATE,
    Power_from_gas_engine double,
    Daily_NEA double,
    Total_Power double,
    Total_power_per_ML double,
    Raw_sew_flow double,
    Raw_sew_com_pH double,
    Raw_sew_com_BOD double,
    Raw_sew_com_COD double,
    Raw_sew_com_TSS double,
    Raw_sew_com_N double,
    Raw_sew_com_OG double,
    Raw_sew_pH double,
    Raw_sew_BOD double,
    Raw_sew_COD double,
    Raw_sew_TSS double,
    Raw_sew_TP double,
    Raw_sew_TColi double,
    Raw_sew_FColi double,
    Grit_TSS double,
    PST_pH double,
    PST_TSS double,
    PST_BOD double,
    PST_COD double,
    PST_Sludge double,
    SC_pH double,
    SC_TSS double,
    SC_BOD double,
    SC_COD double,
    SC_RAS double,
    SST_pH double,
    SST_TSS double,
    SST_BOD double,
    SST_CODRaw_sew double,
    SST_RAS double,
    CCT_com_pH double,
    CCT_com_BOD double,
    CCT_com_COD double,
    CCT_com_TSS double,
    CCT_com_OG double,
    CCT_com_N double,
    CCT_pH double,
    CCT_BOD double,
    CCT_COD double,
    CCT_TSS double,
    CCT_TColi double,
    CCT_FColi double,
    CCT_FRC double,
    Existing_AT_pH double,
    Existing_AT_DO double,
    Existing_AT_MLSS double,
    Existing_AT_MLVSS double,
    Existing_AT_SV30 double,
    Existing_AT_SVI double,
    New_AT_pH double,
    New_AT_DO double,
    New_AT_MLSS double,
    New_AT_MLVSS double,
    New_AT_SV30 double,
    New_AT_SVI double,
    PST_Sludge_pH double,
    PST_Sludge_TS double,
    PST_Sludge_VS double,
    Thickened_sludge_pH double,
    Thickened_sludge_TS double,
    Thickened_sludge_VS double,
    Thickened_sludge_VFA double,
    Thickened_sludge_Alk double,
    Thickened_sludge_VFA_to_Alk double,
    Digestor_Feed_Vol double,
    Digestor_A_pH double,
    Digestor_A_TS double,
    Digestor_A_VS double,
    Digestor_A_VFA double,
    Digestor_A_Alk double,
    Digestor_A_VFA_to_Alk double,
    Digestor_A_Temp double,
    Digestor_B_pH double,
    Digestor_B_TS double,
    Digestor_B_VS double,
    Digestor_B_VFA double,
    Digestor_B_Alk double,
    Digestor_B_VFA_to_Alk double,
    Digestor_B_Temp double,
    Centrifuge_feed_pH double,
    Centrifuge_feed_TS double,
    Centrifuge_feed_VS double,
    Cake_TS double,
    Cake_VS double,
    Centrifuge_feed_Vol double,
    Gas_generation double,
    Methane double,
    Carbondioxide double,
    Gas_scrubber_inlet_pH double,
    Gas_scrubber_outlet_pH double,
    Odour_control_inlet_pH double,
    Odour_control_outlet_pH double
    );
'''

try:
    # Execute the SQL query using the cursor.
    cursor.execute(create_table_query)
    print("Table 'final_data' created successfully!")
except mysql.connector.Error as e:
    print(f"Error: {e}")   
    
#WRITE table:
write_table_query = ''' 
    SET GLOBAL local_infile=1;
    LOAD DATA LOCAL INFILE 'E:\\GWWTP\\WEBSITE\\Processed_data\\data.csv' 
    INTO TABLE final_data
    FIELDS TERMINATED BY ',' 
    OPTIONALLY ENCLOSED BY ''
    LINES TERMINATED BY '\r\n'
    IGNORE 1 LINES; 
'''

try:
    # Execute the SQL query using the cursor.
    cursor.execute(write_table_query)
    print("DATA loaded into 'final_data' successfully!")
except mysql.connector.Error as e:
    print(f"Error: {e}")  

    
#commit    
connection.commit()

# Close the cursor and the database connection.
cursor.close()
connection.close()
