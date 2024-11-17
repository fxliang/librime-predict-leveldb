# librime-predict
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
  # predict db folder in user directory/shared directory
  # default to 'predict.userdb'
  predictdb: predict.userdb
  # max prediction candidates every time
  # default to 0, which means showing all candidates
  # you may set it the same with page_size so that period doesn't trigger next page
  max_candidates: 5
  # max continuous prediction times
  # default to 0, which means no limitation
  max_iterations: 1
```
* Deploy and enjoy.
