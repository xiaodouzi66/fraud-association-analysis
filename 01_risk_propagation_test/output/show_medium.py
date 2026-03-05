import json

with open('/Users/jiangxueyan/Library/Mobile Documents/com~apple~CloudDocs/work-zhongan/2024/10_风控盾/13_风控agent/01_风险传播测试/output/cluster_output.json') as f:
    data = json.load(f)

with open('/Users/jiangxueyan/Library/Mobile Documents/com~apple~CloudDocs/work-zhongan/2024/10_风控盾/13_风控agent/01_风险传播测试/fraud_input.json') as f:
    raw = json.load(f)

seed_case_id = raw['种子案件']['案件号']
mo_scores = data.get('mo_scores', {})

# 中相似: [0.4, 0.6)，剔除种子案件本身（高相似那件就是种子案件对应的案件，已剔除）
medium = {k: v for k, v in mo_scores.items() if 0.4 <= v < 0.6 and k != seed_case_id}
print('中相似案件数（剔除种子）:', len(medium))

# 建立案件号 -> 案件详情 的映射
case_map = {c['案件号']: c for c in raw.get('关联案件列表', []) if '案件号' in c}

print('\n=== 中相似案件详情 ===')
for case_id, mo in sorted(medium.items(), key=lambda x: -x[1]):
    c = case_map.get(case_id)
    if c:
        print(json.dumps({**c, 'mo_score': mo}, ensure_ascii=False, indent=2))
    else:
        print(f'{case_id} (MO={mo}): 案件详情未找到')
    print()
