import sys
import boto3
import io
import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xml.etree.ElementTree as ET
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'json_data'])
json_data = json.loads(args['json_data'])

bucket_name = json_data['bucket_name']
prefix = json_data['prefix']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

glue_client = boto3.client('glue')
s3_client = boto3.client('s3')

def process_group(group, indvl_dict, parent_tag=None):
    if len(group) == 0:
        return

    keys = {key for it in group for key in it.attrib.keys()}

    for key in keys:
        values = [it.attrib.get(key, ' ') for it in group]
        if parent_tag is not None:
            indvl_dict[parent_tag + '_' + group.tag + '_' + key] = values
        else:
            indvl_dict[group.tag + '_' + key] = values
            
def parse_xml(tree, indvl_dicts):
    root = tree.getroot()

    indvls = root[0]

    for indvl in indvls:
        indvl_dict = {}

        info = indvl[0]
        indvl_dict.update({
            'Info_{}'.format(k): v
            for k, v in info.attrib.items()
        })

        for i, group in enumerate(indvl[1:], start=1):
            if i == 2:
                process_group(indvl[i][0][0], indvl_dict, parent_tag=group.tag)
                process_group(indvl[i][0][1], indvl_dict, parent_tag=group.tag)

            elif i == 5:
                if len(indvl[i]) > 0:
                    process_group(indvl[i][0][0], indvl_dict, parent_tag=group.tag)

            process_group(group, indvl_dict)

        indvl_dicts.append(indvl_dict)


xml_files = []

for obj in s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)['Contents']:
    if obj['Key'].endswith('.xml'):
        xml_files.append(obj['Key'])

indvl_dicts = []
for xml_file in xml_files:
    obj = s3_client.get_object(Bucket=bucket_name, Key=xml_file)
    xml_content = obj['Body'].read()
    tree = ET.ElementTree(ET.fromstring(xml_content))
    parse_xml(tree, indvl_dicts)

header = []
keys = set()

for indvl_dict in indvl_dicts:
    for key in indvl_dict.keys():
        if key not in keys:
            header.append(key)
            keys.add(key)
            
output_path = os.path.join(prefix, 'indvls.parquet')

df = pd.DataFrame(indvl_dicts, columns=header)
table = pa.Table.from_pandas(df)

buffer = io.BytesIO()
pq.write_table(table, buffer, compression='zstd')
buffer.seek(0)

s3_client.upload_fileobj(buffer, 'iapd-parquet-data', output_path)

job.commit()

#-------------------
#     OLD CODE     l
# ------------------

# db_name = "parquet-data"

# indvl_mappings = [("crntemps.CrntEmp.BrnchOfLocs.BrnchOfLoc._VALUE", "string", "crntemps.CrntEmp.BrnchOfLocs.BrnchOfLoc._VALUE", "string"), ("crntemps.CrntEmp.BrnchOfLocs.BrnchOfLoc._city", "string", "crntemps.CrntEmp.BrnchOfLocs.BrnchOfLoc._city", "string"), ("crntemps.CrntEmp.BrnchOfLocs.BrnchOfLoc._cntry", "string", "crntemps.CrntEmp.BrnchOfLocs.BrnchOfLoc._cntry", "string"), ("crntemps.CrntEmp.BrnchOfLocs.BrnchOfLoc._postlCd", "string", "crntemps.CrntEmp.BrnchOfLocs.BrnchOfLoc._postlCd", "string"), ("crntemps.CrntEmp.BrnchOfLocs.BrnchOfLoc._state", "string", "crntemps.CrntEmp.BrnchOfLocs.BrnchOfLoc._state", "string"), ("crntemps.CrntEmp.BrnchOfLocs.BrnchOfLoc._str1", "string", "crntemps.CrntEmp.BrnchOfLocs.BrnchOfLoc._str1", "string"), ("crntemps.CrntEmp.BrnchOfLocs.BrnchOfLoc._str2", "string", "crntemps.CrntEmp.BrnchOfLocs.BrnchOfLoc._str2", "string"), ("crntemps.CrntEmp.CrntRgstns.CrntRgstn._VALUE", "string", "crntemps.CrntEmp.CrntRgstns.CrntRgstn._VALUE", "string"), ("crntemps.CrntEmp.CrntRgstns.CrntRgstn._regAuth", "string", "crntemps.CrntEmp.CrntRgstns.CrntRgstn._regAuth", "string"), ("crntemps.CrntEmp.CrntRgstns.CrntRgstn._regCat", "string", "crntemps.CrntEmp.CrntRgstns.CrntRgstn._regCat", "string"), ("crntemps.CrntEmp.CrntRgstns.CrntRgstn._st", "string", "crntemps.CrntEmp.CrntRgstns.CrntRgstn._st", "string"), ("crntemps.CrntEmp.CrntRgstns.CrntRgstn._stDt", "string", "crntemps.CrntEmp.CrntRgstns.CrntRgstn._stDt", "string"), ("crntemps.CrntEmp._city", "string", "crntemps.CrntEmp._city", "string"), ("crntemps.CrntEmp._cntry", "string", "crntemps.CrntEmp._cntry", "string"), ("crntemps.CrntEmp._orgNm", "string", "crntemps.CrntEmp._orgNm", "string"), ("crntemps.CrntEmp._orgPK", "int", "crntemps.CrntEmp._orgPK", "int"), ("crntemps.CrntEmp._postlCd", "string", "crntemps.CrntEmp._postlCd", "string"), ("crntemps.CrntEmp._state", "string", "crntemps.CrntEmp._state", "string"), ("crntemps.CrntEmp._str1", "string", "crntemps.CrntEmp._str1", "string"), ("crntemps.CrntEmp._str2", "string", "crntemps.CrntEmp._str2", "string"), ("drps.DRP._VALUE", "string", "drps.DRP._VALUE", "string"), ("drps.DRP._hasBankrupt", "string", "drps.DRP._hasBankrupt", "string"), ("drps.DRP._hasBond", "string", "drps.DRP._hasBond", "string"), ("drps.DRP._hasCivilJudc", "string", "drps.DRP._hasCivilJudc", "string"), ("drps.DRP._hasCriminal", "string", "drps.DRP._hasCriminal", "string"), ("drps.DRP._hasCustComp", "string", "drps.DRP._hasCustComp", "string"), ("drps.DRP._hasInvstgn", "string", "drps.DRP._hasInvstgn", "string"), ("drps.DRP._hasJudgment", "string", "drps.DRP._hasJudgment", "string"), ("drps.DRP._hasRegAction", "string", "drps.DRP._hasRegAction", "string"), ("drps.DRP._hasTermination", "string", "drps.DRP._hasTermination", "string"), ("dsgntns.Dsgntn._VALUE", "string", "dsgntns.Dsgntn._VALUE", "string"), ("dsgntns.Dsgntn._dsgntnNm", "string", "dsgntns.Dsgntn._dsgntnNm", "string"), ("emphss.EmpHs._VALUE", "string", "emphss.EmpHs._VALUE", "string"), ("emphss.EmpHs._city", "string", "emphss.EmpHs._city", "string"), ("emphss.EmpHs._fromDt", "string", "emphss.EmpHs._fromDt", "string"), ("emphss.EmpHs._orgNm", "string", "emphss.EmpHs._orgNm", "string"), ("emphss.EmpHs._state", "string", "emphss.EmpHs._state", "string"), ("emphss.EmpHs._toDt", "string", "emphss.EmpHs._toDt", "string"), ("exms.Exm._VALUE", "string", "exms.Exm._VALUE", "string"), ("exms.Exm._exmCd", "string", "exms.Exm._exmCd", "string"), ("exms.Exm._exmDt", "string", "exms.Exm._exmDt", "string"), ("exms.Exm._exmNm", "string", "exms.Exm._exmNm", "string"), ("info._VALUE", "string", "info._VALUE", "string"), ("info._actvAGReg", "string", "info._actvAGReg", "string"), ("info._firstNm", "string", "info._firstNm", "string"), ("info._indvlPK", "int", "info._indvlPK", "int"), ("info._lastNm", "string", "info._lastNm", "string"), ("info._link", "string", "info._link", "string"), ("info._midNm", "string", "info._midNm", "string"), ("info._sufNm", "string", "info._sufNm", "string"), ("othrbuss.OthrBus._VALUE", "string", "othrbuss.OthrBus._VALUE", "string"), ("othrbuss.OthrBus._desc", "string", "othrbuss.OthrBus._desc", "string"), ("othrnms.OthrNm._VALUE", "string", "othrnms.OthrNm._VALUE", "string"), ("othrnms.OthrNm._firstNm", "string", "othrnms.OthrNm._firstNm", "string"), ("othrnms.OthrNm._lastNm", "string", "othrnms.OthrNm._lastNm", "string"), ("othrnms.OthrNm._midNm", "string", "othrnms.OthrNm._midNm", "string"), ("othrnms.OthrNm._sufNm", "string", "othrnms.OthrNm._sufNm", "string"), ("prevrgstns.PrevRgstn.BrnchOfLocs.BrnchOfLoc._VALUE", "string", "prevrgstns.PrevRgstn.BrnchOfLocs.BrnchOfLoc._VALUE", "string"), ("prevrgstns.PrevRgstn.BrnchOfLocs.BrnchOfLoc._city", "string", "prevrgstns.PrevRgstn.BrnchOfLocs.BrnchOfLoc._city", "string"), ("prevrgstns.PrevRgstn.BrnchOfLocs.BrnchOfLoc._state", "string", "prevrgstns.PrevRgstn.BrnchOfLocs.BrnchOfLoc._state", "string"), ("prevrgstns.PrevRgstn._orgNm", "string", "prevrgstns.PrevRgstn._orgNm", "string"), ("prevrgstns.PrevRgstn._orgPK", "int", "prevrgstns.PrevRgstn._orgPK", "int"), ("prevrgstns.PrevRgstn._regBeginDt", "string", "prevrgstns.PrevRgstn._regBeginDt", "string"), ("prevrgstns.PrevRgstn._regEndDt", "string", "prevrgstns.PrevRgstn._regEndDt", "string"), ("date", "string", "date", "string")]


# # def resolve(dynamic_frame):
# #     # resolved_df = ResolveChoice.apply(
# #     #     frame=dynamic_frame,
# #     #     choice="make_struct",
# #     #     transformation_ctx="resolved_df"
# #     # )
    
    
# #     return transformed_dynamic_frame

# # def coalesce_df(dynamic_frame):
# #     data_frame = dynamic_frame.toDF()
# #     coalesced_df = data_frame.coalesce(1)
# #     return DynamicFrame.fromDF(coalesced_df, glueContext, "final_dynamic_frame")


# # indvls_df = glueContext.create_dynamic_frame.from_catalog(
# #     database=db_name,
# #     table_name="individuals",
# #     transformation_ctx="indvls_dynamic_frame",
# #     additional_options={"jobBookmarkKeys": ["date"], "jobBookmarkKeysSortOrder": "asc"}
# # )

# # print("original schema: ")
# # indvls_df.printSchema()

# # if indvls_df.count() > 0:
# #     resolved_indvls = resolve(indvls_df)
# #     # print("after resolving: ")
# #     # resolved_indvls.printSchema()

# #     apply_mapping_indvls = ApplyMapping.apply(
# #         frame=resolved_indvls,
# #         mappings=indvl_mappings,
# #         transformation_ctx="apply_mapping_indvls"
# #     )

# #     indvls_final = coalesce_df(apply_mapping_indvls)
    
# #     glueContext.write_dynamic_frame.from_options(
# #         frame=indvls_final,
# #         connection_type="s3",
# #         format="parquet",
# #         connection_options={"path": "s3://iapd-parquet-data/individuals/", "partitionKeys": ["date"]},
# #         format_options={"compression": "snappy"},
# #         transformation_ctx="indvls_output"
# #     )

# def flatten_df(nested_df: DataFrame) -> DataFrame:
#     # Iteratively flatten the DataFrame
#     def flatten_once(df: DataFrame) -> DataFrame:
#         flat_columns = []
#         explode_columns = []
#         for field in df.schema.fields:
#             field_name = field.name
#             dtype = field.dataType
#             if isinstance(dtype, StructType):
#                 for subfield in dtype.fields:
#                     flat_columns.append(col(f"{field_name}.{subfield.name}").alias(f"{field_name}_{subfield.name}"))
#             elif isinstance(dtype, ArrayType):
#                 if isinstance(dtype.elementType, StructType):
#                     explode_columns.append(field_name)
#                 else:
#                     flat_columns.append(col(field_name))
#             else:
#                 flat_columns.append(col(field_name))

#         for col_name in explode_columns:
#             df = df.withColumn(col_name, explode_outer(col(col_name)))

#         return df.select(flat_columns + [col_name for col_name in explode_columns])

#     # Repeatedly flatten until all nested structures are resolved
#     while any(isinstance(field.dataType, (StructType, ArrayType)) for field in nested_df.schema.fields):
#         nested_df = flatten_once(nested_df)
    
#     return nested_df

# df = spark.read.format('xml').options(rowTag='Indvl').load("s3://iapd-xml-data/individuals/date=2024-06-20/*.xml")

# df.printSchema()

# flat_df = flatten_df(df)

# flat_df.show()

# combined_df = flat_df.coalesce(1)

# combined_df.printSchema()

# dynamic_frame = DynamicFrame.fromDF(combined_df, glueContext, "dynamic_frame")

# glueContext.write_dynamic_frame.from_options(
#         frame=dynamic_frame,
#         connection_type="s3",
#         format="parquet",
#         connection_options={"path": "s3://iapd-parquet-data/individuals/date=2024-06-20/", "partitionKeys": []},
#         format_options={"compression": "snappy"},
#         transformation_ctx="indvls_output"
#     )

# job.commit()