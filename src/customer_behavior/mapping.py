import json
from openai import OpenAI
import findspark
from dotenv import load_dotenv
import os
import time
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.window import Window 
# Spark Session
spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

#Function to ETL Log Data
def process_log_search_data(df):
    df = df.select('user_id','keyword')
    df = df.filter((col('user_id').isNotNull()) & (col('keyword').isNotNull())  )
    df = df.groupBy('user_id','keyword').count().withColumnRenamed('count','frequency')
    w = Window.partitionBy('user_id').orderBy(col('frequency').desc())
    df = df.withColumn("rank",row_number().over(w))
    df = df.filter(col('rank') == 1)
    df = df.select('user_id','keyword').withColumnRenamed('keyword','most_searched')
    df = df.orderBy('user_id',ascending = False )
    return df


df_06  = spark.read.parquet("/Users/vinnymac/Library/CloudStorage/OneDrive-Personal/Dataset/log_search/20220601")
df_07 = spark.read.parquet("/Users/vinnymac/Library/CloudStorage/OneDrive-Personal/Dataset/log_search/20220714")

ms_06 = process_log_search_data(df_06)
ms_07 = process_log_search_data(df_07)
data = ms_06.union(ms_07).select('most_searched').distinct()
output_path = "/Users/vinnymac/Documents/Dev/Apache-Spark/Spark/DE Project/code/Customer Behavior/map_output/mapping.csv"


sample_pdf = data.toPandas()

# OpenAI client
load_dotenv("open_ai_api.env")
client = OpenAI()


def classify_batch(movie_list):
    if not movie_list: # check if movie list is empty
        return {}
        
    prompt = f"""
    Bạn là một chuyên gia phân loại nội dung phim, chương trình truyền hình và các loại nội dung giải trí.  
    Bạn sẽ nhận một danh sách tên có thể viết sai, viết liền không dấu, viết tắt, hoặc chỉ là cụm từ liên quan đến nội dung.

    ⚠️ Nguyên tắc quan trọng:
    - Không được trả về "Other" nếu có thể đoán được dù chỉ một phần ý nghĩa.  
    - Luôn cố gắng sửa lỗi, nhận diện tên gần đúng hoặc đoán thể loại gần đúng.  
    - Nếu không chắc → chọn thể loại gần nhất (VD: từ mô tả tình cảm → Romance, tên địa danh thể thao → Sports, chương trình giải trí → Reality Show, v.v.)

    Nhiệm vụ của bạn:
    1. **Chuẩn hoá tên**: thêm dấu tiếng Việt nếu cần, tách từ, chỉnh chính tả (vd: "thuyếtminh" → "Thuyết minh", "tramnamu" → "Trăm năm hữu duyên", "capdoi" → "Cặp đôi").
    2. **Nhận diện tên hoặc ý nghĩa gốc gần đúng nhất**. Bao gồm:
    - Tên phim, series, show, chương trình
    - Quốc gia / đội tuyển (→ "Sports" hoặc "News")
    - Từ khoá mô tả nội dung (→ phân loại theo ý nghĩa, ví dụ "thuyếtminh" → "Other" hoặc "Drama", "bigfoot" → "Horror")
    3. **Gán thể loại phù hợp nhất** trong các nhóm sau:  
    - Action  
    - Romance  
    - Comedy  
    - Horror  
    - Animation  
    - Drama  
    - C Drama  
    - K Drama  
    - Sports  
    - Music  
    - Reality Show  
    - TV Channel  
    - News  
    - Other

    Một số quy tắc gợi ý nhanh:
    - Có từ “VTV”, “HTV”, “Channel” → TV Channel  
    - Có “running”, “master key”, “reality” → Reality Show  
    - Quốc gia, CLB bóng đá, sự kiện thể thao → Sports hoặc News  
    - “sex”, “romantic”, “love” → Romance  
    - “potter”, “hogwarts” → Drama / Fantasy  
    - Tên phim Việt/Trung/Hàn → ưu tiên Drama / C Drama / K Drama

    Chỉ trả về **1 JSON object**.  
    Key = tên gốc trong danh sách.  
    Value = thể loại đã phân loại.

    Ví dụ:  
    {{
    "thuyếtminh": "Other",
    "bigfoot": "Horror",
    "capdoi": "Romance",
    "ARGEN": "Sports",
    "nhật ký": "Drama",
    "PENT": "C Drama",
    "running": "Reality Show",
    "VTV3": "TV Channel"
    }}

    Danh sách:
    {movie_list}

    """

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user","content":prompt}],
            temperature=0
        )
        text = resp.choices[0].message.content.strip()

        # Lấy JSON
        start, end = text.find("{"), text.rfind("}")
        if start == -1 or end == -1:
            return {m: "Other" for m in movie_list}
        parsed = json.loads(text[start:end+1])

        # Map kết quả theo tên phim
        result = {}
        for title in movie_list:
            result[title] = parsed.get(title, "Other")
        return result
        

    except Exception as e:
        print("Error:", e)
        return {m: "Other" for m in movie_list}


def chunk_list(lst, chunk_size=150):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i+chunk_size]

def norm(s):
    return str(s).strip().lower()
# Gọi AI

"""
movies = sample_pdf["most_searched"].dropna().astype(str).tolist()
mapping = classify_batch(movies)
"""
movies = sample_pdf["most_searched"].dropna().astype(str).tolist()

mapping = {}
for idx, batch in enumerate(chunk_list(movies, 150), start=1):
    batch_mapping = classify_batch(batch)
    mapping.update(batch_mapping)

    if idx % 10 == 0:
        print(f"Processed {idx*150} / {len(movies)}")

    time.sleep(0.3)  # avoid rate limits

# Thêm cột Genre duy nhất
sample_pdf["Genre"] = sample_pdf["most_searched"].map(lambda x: mapping.get(x, "Other"))

print(sample_pdf)


sample_pdf.to_csv(output_path, index=False)

print("successfully save as file")
