# NyanTest4SQL

**NyanTest4SQL(にゃんてすと ふぉー えすきゅーえる ふぉー えすきゅーえる)** とは 
NyanQLアプリのSQLファイルをテストするためのCLIツールです。**にゃんてすと  ふぉー えすきゅーえる** と発音します。 

# 主な機能
* NyanQLで実行するSQLファイルをテストするため、テストファイルを自動生成します。
* 用意されたテストファイルを実行できます。
* 実行結果をjunit.xml形式で保存することができます。

# ビルド方法
次のようにビルドしてください。
またリリースしたものをダウンロードしていただくと、ビルド後のバイナリが含まれておりますのでそれをご利用ください。
```
go build -o NyanTest4SQL main.go
```


# 使い方
## テストファイルの自動生成

次のような指定で、テストファイルを自動生成します。

生成先の指定は次のとおりです。
- `-src`：入力（NyanQLの `.sql` があるフォルダ）
- `-out`：テスト定義の出力先（`*.test.jsonc`。params は `-out/_params/` に生成）
- `-expected`：expected（レンダリング結果SQL）の出力先
- `-combine`：実行対象をまとめた `test.json` の出力先

(例)1
```
./NyanTest4SQL gen-sql \
  -src ../../NyanQL/sql \
  -out ./jsonc \
  -expected ./expected \
  -combine ./test.json \
  -auto-expected
```

## 再実行時（2回目以降）の生成ルール

`gen-sql` は、同じSQLに対して何度でも実行できます。再実行時の挙動は次のとおりです。

### 1) 既定の挙動（未生成のみ作成）
`-overwrite` を付けない場合は、**すでに存在するテスト定義（`*.test.jsonc`）は作り直しません**。  
そのため、2回目以降に実行すると基本的に `skip (exists): ...` となり、既存ファイルは変更されません。

- 既存の `*.test.jsonc`：**変更しない（スキップ）**
- 既存の params / expected：**原則そのまま**（テスト定義をスキップするため、更新もしません）

> 「まずは一度生成して、以後は手で params / expected を編集して育てる」運用に向いています。


### 2) 上書きしたい場合（強制再生成）

SQL の変更を反映したい／生成済みのテスト定義や params、expected を作り直したい場合は `-overwrite` を付けて実行します。

(例)2
```bash
./NyanTest4SQL gen-sql \
  -src ../../NyanQL/sql \
  -out ./jsonc \
  -expected ./expected \
  -combine ./test.json \
  -auto-expected \
  -overwrite
```

* -overwrite を付けると、既存の *.test.jsonc や params を 上書き再生成します。
* -auto-expected も付けているため、expected（期待するレンダリング結果SQL）も 上書き再生成されます。
* -combine により ./test.json も毎回生成し直され、テスト一覧が最新化されます。


## 指定したファイルの自動生成
特定の .sql だけ再生成（上書きしない）します。
既にファイルが存在する場合は -overwrite を付けないと skip となります。
未生成の場合は新規作成されます。
再生成の場合は `-overwrite` をつけて実行してください。

(例)3
```bash
./NyanTest4SQL gen-sql \
  -src ../../NyanQL/sql/foo.sql \
  -out ./jsonc \
  -expected ./expected \
  -combine ./test.json \
  -auto-expected
```

(例)4（既存も含めて作り直す場合）
```bash
./NyanTest4SQL gen-sql \
  -src ../../NyanQL/sql/foo.sql \
  -out ./jsonc \
  -expected ./expected \
  -combine ./test.json \
  -auto-expected \
  -overwrite
```  


## テストの実行について
### 全体をテストする

`-config` で `test.json` を指定して実行すると、`test.json` に定義されたテストをすべて実行します。

#### DBにSQLを流さない（レンダリング＋expected比較のみ）
DB へは接続せず、SQLテンプレートのレンダリング結果と expected の一致だけ確認します。

(例)5
```bash
./NyanTest4SQL -config ./test.json -noexec
```

#### DBにSQLを流す（DB実行あり）

DBにSQLを流し正常にSQLが実行できるのかを確認するテストを実行することができます。これはexpected比較に加えて、DBに接続して トランザクション内でSQLを実行します。SQLがDB上で実行できることを確認するためのテストです。

DBに接続して、トランザクション内でSQLを実行します（SQLがDB上で実行できることを確認するためのテストです）。  
既定は **ROLLBACK** のため、永続的な変更は残りません。

実際に開発途中で使っているconfigを利用する場合のコマンドが(例)6になります。
同梱しています config.json.sampleを元にテスト用の設定ファイルをテスト用のDBに接続し動作を確認することもできます。

(例)6
```bash
./NyanTest4SQL -config ./test.json -nyanconf ../../NyanQL/config.json
```


### 1ファイルをテストする

NyanTest4SQL は `test.json` の **テスト名** を単位に実行します。特定のテストだけ実行したい場合は `-only` または `-run` を使います。
（1つの `.sql` から複数のテストが生成される場合があります）

#### テスト名を指定して実行する（-only）

(例)7
```bash
./NyanTest4SQL -config ./test.json -only "list_stamps__by_date"
```

-only はカンマ区切りで複数指定できます。
(例)8 : -only "testA,testB"
```bash
./NyanTest4SQL -config ./test.json -only "testA,testB"
```


#### 正規表現で絞り込む（-run）

(例)9
```bash
./NyanTest4SQL -config ./test.json -run "^list_stamps__"
```

#### DBにSQLを流す（DB実行あり）

(例)10
```bash
./NyanTest4SQL -config ./test.json -nyanconf ../../NyanQL/config.json -only "list_stamps__by_date"
```

### JUnit XML レポート出力する

`-junit-out` を指定すると、テスト結果を **JUnit XML** 形式で指定パスに保存できます。

#### DBにSQLを流さない（レンダリング＋expected比較のみ）

(例)11
```bash
./NyanTest4SQL -config ./test.json -noexec -junit-out ./junit.xml
```

#### DBにSQLを流す（DB実行あり）

(例)12
```bash
./NyanTest4SQL -config ./test.json -nyanconf ../../NyanQL/config.json -junit-out ./junit.xml
```

## ライセンス

本プロジェクトは MIT License の下で公開されています。詳細は `LICENSE` を参照してください。
