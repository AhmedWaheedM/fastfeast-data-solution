## Error Nm1:
- `{"event": "Unsupported format: .json", "level": "warning", "timestamp": "2026-03-26T22:35:57.591501Z"}`
- Explanation : This happen because we wrote `csv: "*.csv", json: "*.json"` and our code read only `csv: ".csv", json: ".json"`
- Solution : edit types **From** --> `csv: "*.csv", json: "*.json"` **To** --> `csv: ".csv", json: ".json"`

## Error Num2:
- `{"exc_info": true, "event": "Error processing cities.json: JSON parse error: Column() changed from object to array in row 0", "level": "error", "timestamp": "2026-03-26T22:51:31.850059Z"}`
- Explanation : `pyarrow.json.read_json()` expects JSON Lines by default, not a **top-level array**.
- Method we added to read `json` :
    ```python
    with open(path, 'r') as f:
            data = json.load(f)
            table = pa.Table.from_pylist(data)
    ```

## Error Num3:
- `{"exc_info": true, "event": "Error processing restaurants.json: Expected bytes, got a 'float' object", "level": "error", "timestamp": "2026-03-26T23:33:43.120368Z"}`
- Explanation : `resturant.json` contains **Nan** values, which **PyArrow** cann't deal with
- Solution : Clean `json` files before processing our data
- Function we added to clena this before processing :
    ```python
    def load_clean_json(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Replace NaN with null
    content = re.sub(r'\bNaN\b', 'null', content)

    return json.loads(content)
    ```