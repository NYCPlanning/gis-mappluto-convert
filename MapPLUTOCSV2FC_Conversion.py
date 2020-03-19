import arcpy as apy
import pandas as pd
import os
import timeit
import datetime
import sys
import traceback
import configparser
import json

try:
    # Set date with datetime to use for properly labeling final outputs

    start_time = timeit.default_timer()
    today_dt = datetime.date.today()
    today = today_dt.strftime('%m_%d_%Y')

    # Define inputs for process

    '''
    VARIABLE EXPLANATIONS
    
    Input CSV - CSV path for PLUTO table generated in Postgresql
    Output CSV - CSV path for processed intermediary table
    Data Path - Path for all non-spatial data inputs and outputs associated with this process
    GDB Path - Path for all spatial data outputs associated with this process
    Blank Table - Text name for final dbf table
    CSV Table - Text name for intermediary dbf table
    Layer Name - Text name for intermediary feature layer
    View Name - Text name for intermediary table view
    Out FC - Text name for final spatial output with date of process included
    Shoreline Date - Text describing date of shoreline file to use for erase method. Can use either a specific date
    or can use newest release. To alter, change the key used on line 463 for the dof_dir dictionary. E.g.
    For the newest shoreline file use dof_dir[newest] or for a specific date use dof_dir[shoreline_date] and alter the 
    variable accordingly
    
    ADDITIONAL INFORMATION
    
    If utilizing a subset for speed gains and quick review set subset_present variable according to the below borough
    dictionary.
    If no subset is being utilized, assign subset_present variable a value of zero
    '''

    # Set configuration file for defining path and credential information
    print("Parsing configuration file")
    config = configparser.ConfigParser()
    config.read(r'mappluto_convert_sample_config.ini')

    # Open log file for outputting start-time, end-time, and uploaded datasets
    print("Assigning log path")
    log_path = config.get('PATHS', 'log_path')
    log = open(log_path, "a")

    # Needs to be changed for each subsequent release version
    version = '19v2'

    boro_dict = {0: '', 1: '_manhattan', 2: '_bronx', 3: '_brooklyn', 4: '_queens', 5: '_staten_island'}

    # Needs to be changed for subset generations, typically for quick turnaround/review
    subset_present = 0

    data_path = config.get('PATHS', 'data_path').format(version, boro_dict[subset_present].replace('_', ''))

    if not os.path.isdir(data_path):
        os.mkdir(data_path)

    print(data_path)

    output_csv = config.get('PATHS', 'output_csv').format(version, boro_dict[subset_present].replace('_', ''),
                                                          boro_dict[subset_present],
                                                          today)
    fgdb_path = config.get('PATHS', 'fgdb_path')

    if not os.path.isdir(fgdb_path):
        os.mkdir(fgdb_path)

    gdb_path_water_area = config.get('PATHS', 'gdb_path_water_area')

    if not apy.Exists(gdb_path_water_area):
        apy.CreateFileGDB_management(fgdb_path, 'MapPLUTO_WaterArea.gdb')

    gdb_path_shoreline_clip = config.get('PATHS', 'gdb_path_shoreline_clip')

    if not apy.Exists(gdb_path_shoreline_clip):
        apy.CreateFileGDB_management(fgdb_path, 'MapPLUTO_ShorelineClip.gdb')

    x_path = config.get('PATHS', 'x_path')

    if not os.path.isdir(x_path):
        os.mkdir(x_path)

    tax_lot_in = os.path.join(gdb_path_water_area, 'Join_File')

    blank_table = "MapPLUTO_final"
    csv_table = "MapPLUTO_intermediary"
    lyr_name = "MapPlutoSpatialLayer"
    view_name = "mappluto_allboro_view"
    out_fc = "MapPLUTO_{}_Water_Included".format(today)


    def original_corrections_export(csv, schema_test, outpath, outname):

        input_csv = csv.format(version, boro_dict[subset_present].replace('_', ''), boro_dict[subset_present])

        # Crawling DOF directory to pull date of latest export for Shoreline and Tax Map Inputs

        dof_dir = {}

        dof_path = config.get('PATHS', 'dof_path')
        apy.env.workspace = dof_path
        exports = apy.ListWorkspaces(None, 'FileGDB')

        print("Listing all available tax map export locations.")
        for workspace in exports:
            workspace_date = workspace.split("_")[3][:-4]
            workspace_date_time = datetime.datetime.strptime(workspace_date, '%Y%m%d')
            print(workspace_date_time)
            dof_dir[workspace_date] = workspace

        newest = max(dof_dir)

        # Reads static csv file into pandas dataframe for field analysis and to validate schema definitions
        # Sets field schemas in dictionary data structure to be applied later upon append of data tables/schema initialization

        print("Reading input csv to obtain field list.")
        mappluto_initial = pd.read_csv(input_csv)
        print("Completed reading input csv.")

        # Load in schema dictionary from external json files

        schema = open(schema_test)
        schema_t = schema.read()
        schema_dict = json.loads(schema_t)

        print(schema_dict)

        print("Writing schema.ini used to properly import output csv into ESRI format with correct data types.")

        # Generate list of fields and field indices for potential troubleshooting

        fields = [field for field in mappluto_initial.columns.values]

        # Define list of fields we expect in static input table that we do not desire in output

        input_field_drop = ['geom', 'mappluto_f', 'rpaddate', 'dcasdate', 'zoningdate', 'landmkdate', 'basempdate',
                            'masdate', 'polidate', 'edesigdate', 'exemptland']

        # Drop undesired fields from input csv

        for field in fields[:]:
            print(field)
            if field in input_field_drop:
                print("Dropping {} field from input csv.".format(field))
                mappluto_initial = mappluto_initial.drop(labels=[field], axis=1)
                print("Removing {}".format(field))
                fields.remove(field)

        indices = [count for count in range(len(fields))]

        # Open schema text file and write header text describing the csv filename

        schema_ini_dict = dict(zip(fields, indices))
        f = open(os.path.join(data_path, "schema.ini"), "w")
        f.write("[" + str(output_csv.split("\\")[-1]) + "]\n")

        converter_dict = {}

        # Write proper data type assignment to converter dictionary to be used to read static
        # csv into pandas dataframe with correct dtypes

        print("Writing schema and converter dictionary to read input csv values with appropriate data types.")

        for field in fields:
            if field not in input_field_drop:
                print("Writing to schema ini - Col{0}={1} {2}\n".format(str(schema_ini_dict[field] + 2), str(field),
                                                                        str(schema_dict[field][1])))
                f.write("Col{0}={1} {2}\n".format(str(schema_ini_dict[field] + 2), str(field), str(schema_dict[field][1])))
                converter_dict[field] = lambda x: str(x)
        f.close()

        print("Converter dict and schema ini complete.")

        print("Reading input csv with converter.")

        # Re-read static csv file into pandas dataframe utilizing converter dictionary

        mappluto_allboro_df = pd.read_csv(input_csv, engine="python", dtype=object) # , converters=converter_dict
        mappluto_allboro_df = mappluto_allboro_df[fields]

        # Export to a new static csv table utilizing schema dictionary

        print("Exporting to output csv.")
        mappluto_allboro_df.to_csv(output_csv)
        print("Export complete.")


        # Remove old table files

        def refresh_gdb(path):
            apy.env.workspace = path
            print("Checking table existence.")
            table_list = apy.ListTables()
            fc_list = apy.ListFeatureClasses()
            print("The following tables are present in your geodatabase:")
            for table in table_list:
                print("Deleting {} from fgdb".format(table))
                delete_table = os.path.join(path, table)
                apy.Delete_management(delete_table)
            for feature in fc_list:
                if "Join" not in feature:
                    print("Deleting {} from fgdb".format(feature))
                    delete_fc = os.path.join(path, feature)
                    apy.Delete_management(delete_fc)
                else:
                    print("{0} is a critical file. "
                          "If you wish to generate a new {0} file, manually delete from fgdb".format(feature))


        refresh_gdb(gdb_path_water_area)
        refresh_gdb(gdb_path_shoreline_clip)
        print("Deletions complete. Re-generating tables.")

        # Create new table files

        # Set appropriate workspace path
        apy.env.workspace = gdb_path_water_area
        # Create blank tables
        print("Creating dbase table with appropriate field names and schema.")
        apy.CreateTable_management(gdb_path_water_area, blank_table)
        print("Table created.")
        print("Creating dbase table with appropriate field names and schema for append.")
        apy.CreateTable_management(gdb_path_water_area, csv_table)
        print("Table created.")


        def create_table(table_name, type):
            # Add field schemas to blank table
            print("Adding the following schemas for field - " + str(item))
            print("Name : " + str(schema_dict[item][0]))
            print("Type : " + type)
            print("Precision : " + str(schema_dict[item][2]))
            print("Scale : " + str(schema_dict[item][3]))
            print("Length : " + str(schema_dict[item][4]))
            print("Alias : " + str(schema_dict[item][5]))
            print("Nullable : " + str(schema_dict[item][6]))

            apy.AddField_management(table_name, schema_dict[item][0],
                                    type,
                                    schema_dict[item][2],
                                    schema_dict[item][3],
                                    schema_dict[item][4],
                                    schema_dict[item][5],
                                    schema_dict[item][6])


        for item in schema_dict:
            print(item)
            # Create target DBase with string schema
            create_table(csv_table, "TEXT")
            # Create target DBase table with pre-defined schema
            create_table(blank_table, str(schema_dict[item][1]))


        # Create input DBase table from field-limited csv dataset

        print("Appending static csv to dbase table")
        apy.Append_management(output_csv, csv_table, "NO_TEST")
        print("Append complete.")

        # Append input DBase table to target DBase table.
        # NO_TEST is important because it forces input dbf to inherit target dbf schema in the appending process

        print("Appending target and input dbase tables to combine correct fields/schema with correct attribute info.")
        apy.Append_management(csv_table, blank_table, "NO_TEST")
        print("Appending tables complete.")

        apy.env.workspace = gdb_path_water_area
        print("Generating DBF table.")
        apy.TableSelect_analysis(blank_table, 'UNMAPPABLES', '"PLUTOMapID" = \'2\' OR "PLUTOMapID" = \'4\'')

        # Check for join polygon in gdb, if it exists, continue, if not, create it.
        print("Checking GDB for Join file")

        gdb_check_list = apy.ListFeatureClasses()
        join_hit_count = 0
        dissolve_hit_count = 0

        for file in gdb_check_list:
            print(file)
            if "Join_File" in file:
                print("Join file present in GDB. Continuing")
                join_hit_count += 1

        if join_hit_count == 0:
            print("Join file not present in GDB. Creating it now.")
            for file in os.listdir(data_path):
                if "dcp_mappluto" in file and file.endswith(".shp"):
                    print("Copying Join file to GDB")
                    apy.FeatureClassToFeatureClass_conversion(os.path.join(data_path, file),
                                                              gdb_path_water_area,
                                                              "Join_File")
        elif join_hit_count == 1:
            print("Join file exists. Skipping")

        # Join in-memory datasets and export to a new FeatureClass -- ShorelineNotClipped

        join_fields = apy.ListFields(tax_lot_in)
        bbl_check = 0

        print("Checking for Double BBL field in Join File")
        for field in join_fields:
            if 'BBL' in field.name.upper() and field.type == 'Double':
                print("Spatial join file contains {} with {} type. Proceeding with join.".format(field.name, field.type))
            elif 'BBL' in field.name.upper() and field.type is not 'Double':
                print("Spatial join file lacks {} with {} type. Generating field now.".format(field.name, field.type))
                apy.AddField_management(tax_lot_in, 'BBL_Dbl', 'DOUBLE')
                apy.CalculateField_management(tax_lot_in, 'BBL_Dbl', '!BBL!', 'PYTHON3')
                bbl_check = 1

        # Repair geometry of join shapefile to eliminate the need to repair across network drives in the future
        print("Repairing geometry for input.")
        apy.RepairGeometry_management(tax_lot_in)

        print("Creating in-memory layer.")
        # Create in-memory layer from DOF Tax Map feature class

        apy.MakeFeatureLayer_management(in_features=tax_lot_in, out_layer=lyr_name)
        print("Creating in-memory table.")
        # Create in-memory table from target DBase table

        apy.MakeTableView_management(in_table=blank_table, out_view=view_name)

        # Join layer to table in-memory on bbl, keep matching only

        if bbl_check == 1:
            print("Joining in-memory table with in-memory layer using double")
            apy.AddJoin_management(in_layer_or_view=lyr_name,
                                   in_field="BBL_Dbl",
                                   join_table=view_name,
                                   join_field="BBL",
                                   join_type="KEEP_COMMON")
        elif bbl_check == 0:
            print("Joining in-memory table with in-memory layer")
            apy.AddJoin_management(in_layer_or_view=lyr_name,
                                   in_field="BBL",
                                   join_table=view_name,
                                   join_field="BBL",
                                   join_type="KEEP_COMMON")

        # Copy layer to feature class

        print("Exporting to FeatureClass")
        apy.CopyFeatures_management(in_features=lyr_name, out_feature_class=out_fc)
        print("Export of new FeatureClass complete")

        # Remove all Tax Map Polygon fields and ObjectID from joined table

        apy.env.workspace = gdb_path_water_area
        output_fields = apy.ListFields(out_fc)
        for field in output_fields:
            if "dcp_mappluto_{}".format(
                    version) in field.name or "MapPLUTO_final_OBJECTID" in field.name or "Join_File" in field.name:
                print("Deleting the following unnecessary fields: {}".format(field.name))
                apy.DeleteField_management(out_fc, field.name)
        print("Field deletion complete")

        # Rename all fields to reflect original MapPluto FeatureClass

        table_name_string = "MapPLUTO_final_"

        field_list = apy.ListFields(out_fc)
        field_count = 0
        for field in field_list:
            if table_name_string in field.name:
                apy.AlterField_management(out_fc, field.name, field.name.replace(table_name_string, ""),
                                          field.name.replace(table_name_string, ""))
                print(field.name + " replaced with " + field.name.replace(table_name_string, ""))
                field_count += 1
                print(str(field_count) + " fields completed out of " + str(len(field_list) - 4))

        shoreline_path = os.path.join(dof_dir[newest], "DCP")
        apy.env.workspace = shoreline_path
        shoreline_list = []
        for fc in apy.ListFeatureClasses():
            if "Shoreline_Polygon" in fc:
                shoreline_list.append(fc)

        print("Adding attribute indices to BBL fields")
        print("Adding index to Water Included")
        apy.AddIndex_management(os.path.join(gdb_path_water_area, out_fc), 'BBL', 'BBL_Water', 'UNIQUE')

        # Create shoreline clipped version of FC

        apy.env.workspace = gdb_path_shoreline_clip

        print("Generating shoreline clipped feature class via erase analysis tool")

        apy.Erase_analysis(os.path.join(gdb_path_water_area, out_fc), os.path.join(shoreline_path, shoreline_list[0]),
                           out_fc.replace("Water_Included", "Shoreline_Clipped"))

        print("Erase complete. Shoreline clipped feature generated")

        print("Adding index to Shoreline Clipped")
        apy.AddIndex_management(
            os.path.join(gdb_path_shoreline_clip, out_fc.replace("Water_Included", "Shoreline_Clipped")), 'BBL',
            'BBL_Shore', 'UNIQUE')
        print("Attribute indices added to BBL fields")

        apy.env.workspace = gdb_path_shoreline_clip
        if apy.Exists('UNMAPPABLES'):
            print("Unmappable DBF already exists. Skipping this step.")
        else:
            print("Generating DBF table.")
            apy.TableSelect_analysis(os.path.join(gdb_path_water_area, blank_table), 'UNMAPPABLES', '"PLUTOMapID" = \'2\'')

        # Outputting final results to X: drive location

        x_version_path = os.path.join(x_path, version)

        if os.path.isdir(x_version_path):
            print("Output version directory exists. Skipping")
        else:
            print("Output version directory does not exist. Creating now.")
            os.mkdir(x_version_path)

        x_version_output_path = os.path.join(x_version_path, 'output')

        if os.path.isdir(x_version_output_path):
            print("Output version directory exists. Skipping")
        else:
            print("Output version directory does not exist. Creating now.")
            os.mkdir(x_version_output_path)

        x_output_gdb_path = os.path.join(x_version_path, 'output', outpath)

        if not os.path.isdir(x_output_gdb_path):
            print("Original or Correction directory does not exist. Creating now.")
            os.mkdir(x_output_gdb_path)
        else:
            print("Original or Correction directory already exists. Skipping")

        apy.env.workspace = x_output_gdb_path
        apy.env.overwriteOutput = True

        print("Outputting MapPLUTO_WaterArea_Included")
        apy.Copy_management(os.path.join(fgdb_path, 'MapPLUTO_WaterArea.gdb'),
                            os.path.join(x_output_gdb_path, 'MapPLUTO_WaterArea_{}_{}.gdb'.format(outname, today)))
        print("Outputting new MapPLUTO_Shoreline_Clippped")
        apy.Copy_management(os.path.join(fgdb_path, 'MapPLUTO_ShorelineClip.gdb'),
                            os.path.join(x_output_gdb_path, 'MapPLUTO_ShorelineClip_{}_{}.gdb'.format(outname, today)))
        print("Outputs complete")

        retain_files = ['MapPLUTO_{}_Shoreline_Clipped'.format(today), 'MapPLUTO_{}_Water_Included'.format(today),
                        'UNMAPPABLES', 'MapPLUTO', 'MapPLUTO_UNCLIPPED', 'NOT_MAPPED_LOTS_UNCLIPPED', 'NOT_MAPPED_LOTS']

        print("Modifying outputs to include only desired files")
        apy.env.workspace = x_output_gdb_path
        for out_gdb_path in apy.ListWorkspaces():
            if today in out_gdb_path:
                apy.env.workspace = out_gdb_path
                output_fc_list = apy.ListFeatureClasses()
                output_table_list = apy.ListTables()
                for fc in output_fc_list:
                    if fc not in retain_files:
                        print("Deleting {}".format(fc))
                        apy.Delete_management(fc)
                    if outname is 'Corrected' and 'MapPLUTO_{}'.format(today) in fc:
                        print("Renaming {} to {}".format(fc, fc.split('.')[0] + '_Corrected'))
                        apy.Rename_management(fc, fc.split('.')[0] + '_Corrected')
                output_fc_list = apy.ListFeatureClasses()
                for tbl in output_table_list:
                    if 'unclipped' in out_gdb_path or 'UNCLIPPED' in output_fc_list[0]:
                        if tbl not in retain_files:
                            print("Deleting {}".format(tbl))
                            apy.Delete_management(tbl)
                        else:
                            print("Renaming {} to {}".format(tbl, 'NOT_MAPPED_LOTS_UNCLIPPED.dbf'))
                            apy.Rename_management(tbl, "NOT_MAPPED_LOTS_UNCLIPPED.dbf")
                    else:
                        if tbl not in retain_files:
                            print("Deleting {}".format(tbl))
                            apy.Delete_management(tbl)
                        else:
                            print("Renaming {} to {}".format(tbl, 'NOT_MAPPED_LOTS.dbf'))
                            apy.Rename_management(tbl, 'NOT_MAPPED_LOTS.dbf')

    # Begin processing standard version of MapPLUTO without correction flag field

    # print("Beginning original MapPLUTO export")
    # original_corrections_export(config.get('PATHS', 'original_input_csv'),
    #                             config.get('PATHS', 'original_schema_path'),
    #                             'originals',
    #                             None)
    # print("Original MapPLUTO export complete.")

    # Begin processing version of MapPLUTO with dcpedit correction flag field

    print("Beginning corrected MapPLUTO export")
    original_corrections_export(config.get('PATHS', 'corrections_input_csv'),
                                config.get('PATHS', 'corrections_schema_path'),
                                'corrections',
                                'Corrected')
    print("Corrected MapPLUTO export complete.")

    print("Script complete. Finished in " + str((timeit.default_timer() - start_time) / 60) + " minutes.")

except:
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]

    pymsg = "PYTHON ERRORS:\nTraceback Info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
    msgs = "ArcPy ERRORS:\n" + apy.GetMessages() + "\n"

    print(pymsg)
    print(msgs)

    log.write("" + pymsg + "\n")
    log.write("" + msgs + "")
    log.write("\n")
    log.close()