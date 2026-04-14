import os
import sys

# 将src目录添加到Python路径（在导入之前）
src_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, src_dir)
from stages.initialize.embedding.search import get_semantic_embedding_search_service

if __name__ == "__main__":
    service = get_semantic_embedding_search_service()
    result = service.search_columns_by_text("工厂的设备编号ID有没有重复的", ["industrial_monitoring"])
    for item in result:
        print(item["database_name"],item["table_name"],item["column_name"],item["similarity"])
