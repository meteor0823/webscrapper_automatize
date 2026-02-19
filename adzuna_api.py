import requests
import time
import json
import pandas as pd
from datetime import datetime

APP_ID = "abfa41a4"
APP_KEY = "35ab524c83cf1d5f5a7566388892f9c5"

def fetch_all_jobs():
    """
    持续翻页获取所有职位数据,直到当前页和上一页结果相同时停止
    """
    all_results = []
    page = 1
    previous_page_results = None
    previous_page_ids = None
    
    while True:
        print(f"正在获取第 {page} 页...")
        
        # 构建URL,页码在URL最后
        url = f"https://api.adzuna.com/v1/api/jobs/us/search/{page}"
        
        params = {
            "app_id": APP_ID,
            "app_key": APP_KEY,
            "results_per_page":100,
            "sort_by:date",
            # "what": "economist",  # 如需搜索特定关键词可取消注释
            "content-type": "application/json"
        }
        
        try:
            # 发送请求
            r = requests.get(url, params=params)
            r.raise_for_status()
            data = r.json()
            
            # 获取当前页的结果
            current_results = data.get('results', [])
            
            # 检查是否与上一页相同
            if previous_page_results is not None:
                if current_results == previous_page_results:
                    print(f"第 {page} 页与第 {page-1} 页结果相同,停止翻页")
                    break
            
            # 检查2: 提取当前页的ID集合
            current_page_ids = set(item['id'] for item in current_results)
  
            # 检查3: 与上一页的ID集合比较（而不是比较整个results）
            if previous_page_ids is not None:
                if current_page_ids == previous_page_ids:
                    print(f"第 {page} 页与第 {page-1} 页的ID完全相同,停止翻页")
                    break

            if page> 5000:
                break

            # 合并结果
            all_results.extend(current_results)
            print(f"第 {page} 页获取了 {len(current_results)} 条结果")
            
            # 保存当前页结果用于下次比较
            previous_page_results = current_results
            previous_page_ids = current_page_ids
            
            # 翻到下一页
            page += 1
            
            # 添加延迟避免请求过快
            time.sleep(0.3)
            
        except requests.exceptions.HTTPError as e:
            print(f"HTTP错误: {e}")
            break
        except requests.exceptions.RequestException as e:
            print(f"请求错误: {e}")
            break
    
    print(f"\n总共获取了 {len(all_results)} 条职位数据")
    return all_results

# 使用示例
if __name__ == "__main__":
    jobs_data = fetch_all_jobs()
    
    # 获取当天日期作为文件名 (格式: YYYYMMDD)
    today = datetime.now().strftime("%Y%m%d")

    # 基础路径
    # base_path = "/Users/beneductxu/Desktop/AllDocument/PhD_Course/TA/Development_TA"

    # 构建完整文件路径
    json_file = f"{today}.json"
    csv_file = f"{today}.csv"

    # ====== 保存 JSON ======
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(jobs_data, f, ensure_ascii=False, indent=2)
    print(f"JSON数据已保存到 {json_file}")

    # ====== 转换为 DataFrame 并保存 CSV ======
    df = pd.json_normalize(jobs_data)  # jobs_data 已经是 results 列表
    df.to_csv(csv_file, index=False, encoding='utf-8')
    print(f"CSV数据已保存到 {csv_file}")
    print(f"共保存 {len(df)} 条职位数据")
