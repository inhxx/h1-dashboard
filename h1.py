#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
H1目标完成度 - 最终版CSV转JSON
使用方法: python3 csv_to_json_final.py Week15_数据填报.csv
"""

import pandas as pd
import json
import sys
from datetime import datetime

TOTAL_WEEKS = 26

# 目标配置（郭楚怡已更新）
TARGETS_CONFIG = {
    "刘慧慧": {
        "type": "私域",
        "channels": 1,
        "targets": [{"name": "私域运营", "target1": 900, "target2": 60000, "target3": 1500, "weight1": 0.5, "weight2": 0.25, "weight3": 0.25}]
    },
    "郭楚怡": {  # 已更新：目标1正价750(40%)，目标2Leads500(30%)，目标3企微2000(30%)
        "type": "公域",
        "channels": 1,
        "targets": [{"name": "公域运营", "target1": 750, "target2": 500, "target3": 2000, "weight1": 0.4, "weight2": 0.3, "weight3": 0.3}]
    },
    "张克楠": {
        "type": "转化运营",
        "isMultiChannel": True,
        "channelWeights": [0.407, 0.165, 0.113, 0.259, 0.045, 0.011],
        "targets": [
            {"name": "网校投放", "target1": 12974, "target2": 100000, "target3": 232, "weight1": 0.4, "weight2": 0.3, "weight3": 0.3},
            {"name": "网校站内", "target1": 5272, "target2": 100000, "target3": 270, "weight1": 0.4, "weight2": 0.3, "weight3": 0.3},
            {"name": "xwx站内", "target1": 3610, "target2": 100000, "target3": 345, "weight1": 0.4, "weight2": 0.3, "weight3": 0.3},
            {"name": "yk站内", "target1": 8254, "target2": 100000, "target3": 241, "weight1": 0.4, "weight2": 0.3, "weight3": 0.3},
            {"name": "应用商店", "target1": 1437, "target2": 100000, "target3": 170, "weight1": 0.4, "weight2": 0.3, "weight3": 0.3},
            {"name": "活动增长", "target1": 350, "target2": 100000, "target3": 262, "weight1": 0.4, "weight2": 0.3, "weight3": 0.3}
        ]
    },
    "马俊波": {
        "type": "转化运营",
        "isMultiChannel": True,
        "channelWeights": [0.582, 0.418],
        "targets": [
            {"name": "特训-商务X", "target1": 2404, "target2": 60000, "target3": 260, "weight1": 0.4, "weight2": 0.3, "weight3": 0.3},
            {"name": "特训-商务社群", "target1": 1729, "target2": 60000, "target3": 180, "weight1": 0.4, "weight2": 0.3, "weight3": 0.3}
        ]
    },
    "基莉": {
        "type": "转化运营",
        "isMultiChannel": True,
        "channelWeights": [0.433, 0.316, 0.251],
        "targets": [
            {"name": "特训-商务-B站", "target1": 1470, "target2": 60000, "target3": 210, "weight1": 0.4, "weight2": 0.3, "weight3": 0.3},
            {"name": "特训-商务-KOC", "target1": 1076, "target2": 54000, "target3": 170, "weight1": 0.4, "weight2": 0.3, "weight3": 0.3},
            {"name": "特训-商务-大搜", "target1": 1322, "target2": 60000, "target3": 450, "weight1": 0.4, "weight2": 0.3, "weight3": 0.3}
        ]
    },
    "徐颖": {
        "type": "转化运营",
        "channels": 1,
        "targets": [{"name": "私海热点", "target1": 36805, "target2": 35000, "target3": 2.6, "weight1": 0.4, "weight2": 0.3, "weight3": 0.3}]
    },
    "杜海奎": {
        "type": "前端渠道",
        "isMultiChannel": True,
        "channelWeights": [0.278, 0.636, 0.071, 0.015],
        "targets": [
            {"name": "xwx站内", "target1": 3610, "target2": 1.7, "target3": 15301, "weight1": 0.4, "weight2": 0.2, "weight3": 0.4},
            {"name": "yk站内", "target1": 8254, "target2": 1.7, "target3": 48779, "weight1": 0.4, "weight2": 0.2, "weight3": 0.4},
            {"name": "应用商店", "target1": 1437, "target2": 1.7, "target3": 9695, "weight1": 0.4, "weight2": 0.2, "weight3": 0.4},
            {"name": "活动增长", "target1": 350, "target2": 1.7, "target3": 1776, "weight1": 0.4, "weight2": 0.2, "weight3": 0.4}
        ]
    },
    "郭思琪": {
        "type": "前端渠道",
        "isMultiChannel": True,
        "channelWeights": [0.758, 0.242],
        "targets": [
            {"name": "特训-商务X", "target1": 4133, "target2": 1.1, "target3": 10654, "weight1": 0.3, "weight2": 0.4, "weight3": 0.3},
            {"name": "特训-商务-大搜", "target1": 1322, "target2": 1.4, "target3": 16800, "weight1": 0.3, "weight2": 0.4, "weight3": 0.3}
        ]
    },
    "王佳琦": {
        "type": "前端渠道",
        "channels": 1,
        "targets": [{"name": "特训-商务-B站", "target1": 1470, "target2": 1.3, "target3": 19830, "weight1": 0.3, "weight2": 0.4, "weight3": 0.3}]
    },
    "贾鹏飞": {
        "type": "前端渠道",
        "channels": 1,
        "targets": [{"name": "特训-商务-KOC", "target1": 1076, "target2": 1.2, "target3": 6200, "weight1": 0.3, "weight2": 0.4, "weight3": 0.3}]
    }
}

def calculate_completion(name, actual_data):
    """计算总完成度"""
    config = TARGETS_CONFIG[name]
    targets = config["targets"]
    
    if config.get("isMultiChannel"):
        # 多Channel：先算每个Channel完成度，再加权平均
        channel_completions = []
        for i, target in enumerate(targets):
            actual = actual_data[i]
            c1 = (actual["actual1"] / target["target1"]) * target["weight1"]
            c2 = (actual["actual2"] / target["target2"]) * target["weight2"] if target["target2"] else 0
            c3 = (actual["actual3"] / target["target3"]) * target["weight3"] if target["target3"] else 0
            channel_completions.append(c1 + c2 + c3)
        
        # 加权平均
        weights = config["channelWeights"]
        total = sum(c * w for c, w in zip(channel_completions, weights))
        return min(total * 100, 150)
    else:
        # 单Channel：直接加权
        target = targets[0]
        actual = actual_data[0]
        c1 = (actual["actual1"] / target["target1"]) * target["weight1"]
        c2 = (actual["actual2"] / target["target2"]) * target["weight2"] if target.get("target2") else 0
        c3 = (actual["actual3"] / target["target3"]) * target["weight3"] if target.get("target3") else 0
        return min((c1 + c2 + c3) * 100, 150)

def calculate_dimensions(name, actual_data):
    """计算3个维度进度"""
    config = TARGETS_CONFIG[name]
    targets = config["targets"]
    
    if config.get("isMultiChannel"):
        dim1_total, dim2_total, dim3_total = 0, 0, 0
        weights = config["channelWeights"]
        
        for i, target in enumerate(targets):
            actual = actual_data[i]
            dim1_total += (actual["actual1"] / target["target1"]) * weights[i]
            if target.get("target2"):
                dim2_total += (actual["actual2"] / target["target2"]) * weights[i]
            if target.get("target3"):
                dim3_total += (actual["actual3"] / target["target3"]) * weights[i]
        
        return {
            "dim1": round(dim1_total * 100, 1),
            "dim2": round(dim2_total * 100, 1) if dim2_total > 0 else "-",
            "dim3": round(dim3_total * 100, 1) if dim3_total > 0 else "-"
        }
    else:
        target = targets[0]
        actual = actual_data[0]
        dim1 = (actual["actual1"] / target["target1"]) * 100
        dim2 = (actual["actual2"] / target["target2"]) * 100 if target.get("target2") else "-"
        dim3 = (actual["actual3"] / target["target3"]) * 100 if target.get("target3") else "-"
        return {"dim1": round(dim1, 1), "dim2": round(dim2, 1) if dim2 != "-" else "-", "dim3": round(dim3, 1) if dim3 != "-" else "-"}

def calculate_health(total_completion, current_week):
    """计算健康度（基于总完成度）"""
    time_progress = current_week / TOTAL_WEEKS
    health_score = (total_completion / 100) / time_progress * 100 if time_progress > 0 else 0
    
    if health_score >= 110:
        return {"score": round(health_score, 1), "status": "超前", "status_code": "excellent", "class_name": "health-excellent"}
    elif health_score >= 95:
        return {"score": round(health_score, 1), "status": "正常", "status_code": "good", "class_name": "health-good"}
    elif health_score >= 80:
        return {"score": round(health_score, 1), "status": "预警", "status_code": "warning", "class_name": "health-warning"}
    elif health_score >= 60:
        return {"score": round(health_score, 1), "status": "滞后", "status_code": "danger", "class_name": "health-danger"}
    else:
        return {"score": round(health_score, 1), "status": "严重滞后", "status_code": "critical", "class_name": "health-critical"}

def csv_to_json(csv_file, output_file='data.json'):
    """主函数"""
    print(f"📖 读取CSV: {csv_file}")
    
    try:
        df = pd.read_csv(csv_file, encoding='utf-8')
    except:
        df = pd.read_csv(csv_file, encoding='gbk')
    
    import re
    match = re.search(r'Week(\d+)', csv_file)
    current_week = int(match.group(1)) if match else 14
    date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"📅 Week {current_week}/{TOTAL_WEEKS}, {date}\n")
    
    people_data = []
    
    for name in TARGETS_CONFIG.keys():
        config = TARGETS_CONFIG[name]
        person_targets = []
        actual_data_list = []
        
        for target in config["targets"]:
            channel_name = target["name"]
            row = df[(df['name'] == name) & (df['channel'] == channel_name)]
            
            if row.empty:
                actual1, actual2, actual3 = 0, 0, 0
            else:
                actual1 = float(row.iloc[0]['actual1']) if pd.notna(row.iloc[0]['actual1']) else 0
                actual2 = float(row.iloc[0]['actual2']) if 'actual2' in row and pd.notna(row.iloc[0]['actual2']) else 0
                actual3 = float(row.iloc[0]['actual3']) if 'actual3' in row and pd.notna(row.iloc[0]['actual3']) else 0
            
            actual_data_list.append({"actual1": actual1, "actual2": actual2, "actual3": actual3})
            
            target_data = {"name": channel_name, "target": target["target1"], "actual": int(actual1)}
            if target.get("target2"):
                target_data["target2"] = target["target2"]
                target_data["actual2"] = actual2
            if target.get("target3"):
                target_data["target3"] = target["target3"]
                target_data["actual3"] = actual3
            
            person_targets.append(target_data)
        
        total_completion = calculate_completion(name, actual_data_list)
        dimensions = calculate_dimensions(name, actual_data_list)
        health = calculate_health(total_completion, current_week)
        
        people_data.append({
            "name": name,
            "type": config["type"],
            "channels": len(config["targets"]),
            "targets": person_targets,
            "total_completion": round(total_completion, 1),
            "dim1_progress": dimensions["dim1"],
            "dim2_progress": dimensions["dim2"],
            "dim3_progress": dimensions["dim3"],
            "health": health
        })
        
        status_emoji = "🟢" if health["status_code"] == "excellent" else "🔵" if health["status_code"] == "good" else "🟡" if health["status_code"] == "warning" else "🟠" if health["status_code"] == "danger" else "🔴"
        print(f"{status_emoji} {name}: 完成{total_completion:.1f}% | 健康{health['score']:.1f}% | {health['status']}")
    
    output = {"currentWeek": current_week, "totalWeeks": TOTAL_WEEKS, "date": date, "data": people_data}
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"\n🎉 已生成 {output_file}")
    return output

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法: python3 csv_to_json_final.py Week15_数据填报.csv")
        sys.exit(1)
    
    result = csv_to_json(sys.argv[1])
    
    print("\n🏆 排名:")
    sorted_data = sorted(result['data'], key=lambda x: x['health']['score'], reverse=True)
    for i, person in enumerate(sorted_data, 1):
        status_emoji = "🟢" if person['health']['status_code'] == "excellent" else "🔵" if person['health']['status_code'] == "good" else "🟡" if person['health']['status_code'] == "warning" else "🟠" if person['health']['status_code'] == "danger" else "🔴"
        print(f"{i}. {status_emoji} {person['name']}: 健康度{person['health']['score']}% | 完成{person['total_completion']}% | {person['health']['status']}")
