# librime-predict-leveldb
librime plugin. predict next word by commit history.

a mod of `rime/librime-predict`

## Usage
* In `*.schema.yaml`, add `predictor` to the list of `engine/processors` before `key_binder`,
add `predict_translator` to the list of `engine/translators`;
or patch the schema with:
```yaml
patch:
  'engine/processors/@before 0': predictor
  'engine/translators/@before 0': predict_translator
```

* Add the `prediction` switch:
```yaml
switches:
  - name: prediction
    states: [ 關閉預測, 開啓預測 ]
    reset: 1
```
* Config items for your predictor:
```yaml
predictor:
  # predict db folder in user directory
  # default to 'predict.userdb'
  # be careful when there is a schema named `predict`, in which case you should reset value of this `predictdb` key to another name for better compcompliance.
  predictdb: predict.userdb
  # max prediction candidates every time
  # default to 0, which means showing all candidates
  # you may set it the same with page_size so that period doesn't trigger next page
  max_candidates: 5
  # max continuous prediction times
  # default to 0, which means no limitation
  max_iterations: 1
  # 提交时间间隔阈值（秒）
  # 超过此时间间隔的两次提交，将不会被认为有关联
  # 用于避免将长时间间隔后的输入错误地关联在一起
  # 默认值：30 秒
  # 设为 0 或负值可禁用时间限制（恢复旧版行为，始终关联所有提交）
  # 建议范围：10-120 秒，根据实际使用习惯调整
  max_commit_interval_seconds: 30
  # 兼容模式：启用后按 librime-predict 的方式预测候选
  # - 不做 leveldb 扩展的提交关系学习与删除更新
  # - 仅按最近提交词进行预测
  # - 数据源切换为用户目录下的 predictor/db（默认 predict.db）
  # 默认值：false（使用 librime-predict-leveldb 现有逻辑）
  legacy_mode: false
  # legacy_mode=true 时生效，默认 predict.db
  db: predict.db
```
* Deploy and enjoy.

## Data conversion tool

Build target `predict_data_tool` currently supports conversion between leveldb
(`predict.userdb`) and txt.

Python version is also provided at `scripts/predict_data_tool.py` (requires
`plyvel`).

```bash
# python tool (recommended for quick use)
pip install plyvel
python3 scripts/predict_data_tool.py \
  --from leveldb --to txt --input ./predict.userdb --output ./predict.txt

# leveldb -> txt
./build/plugins/librime-predict-leveldb/predict_data_tool \
  --from leveldb --to txt --input ./predict.userdb --output ./predict.txt

# txt -> leveldb
./build/plugins/librime-predict-leveldb/predict_data_tool \
  --from txt --to leveldb --input ./predict.txt --output ./predict.userdb
```

txt format (tab-separated):

```text
prefix<TAB>word<TAB>weight
# or
prefix<TAB>word<TAB>weight<TAB>commits<TAB>dee<TAB>tick
```
