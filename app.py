# backend/app.py

from flask import Flask, request, send_file
import pandas as pd
import re
import os
import io
from flask_cors import CORS
from zipfile import ZipFile

app = Flask(__name__)
CORS(app)

protected_categories = {
    "400-4000-4581", "400-4000-4582", "700-7800-0118", "800-8100-0172",
    "400-4100-0038", "400-4100-0039", "700-7900-0130", "800-8100-0164",
    "800-8600-0193", "900-9100-0214", "800-8200-0173", "400-4100-0226",
    "800-8000-0159", "800-8000-0325", "800-8100-0165", "700-7800-0120",
    "900-9100-0215", "900-9100-0216", "400-4100-0047", "400-4100-0037"
}

def is_valid_placeid(val):
    return isinstance(val, str) and len(val.strip()) == 41

def is_valid_category_id(val):
    return bool(re.fullmatch(r'\d{3}-\d{4}-\d{4}', str(val).strip()))

def is_valid_float(val):
    try:
        float(val)
        return True
    except:
        return False

@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files.get('file')
    type_ = request.form.get('type', '').lower()
    if not file or not type_:
        return "Missing file or type", 400

    df = pd.read_csv(file, dtype=str)
    duplicate_placeid_mask = df.duplicated(subset=["PLACEID"], keep=False)

    valid_rows = []
    invalid_rows = []

    for idx, row in df.iterrows():
        errors = {}
        placeid = str(row.get("PLACEID", "")).strip()

        if not is_valid_placeid(placeid):
            errors["PLACEID"] = "Must be exactly 41 characters, no spaces or nulls"

        if str(row.get("CHANGETYPE", "")).strip() != "UPDATE":
            errors["CHANGETYPE"] = "Must be 'UPDATE'"

        attribute = str(row.get("ATTRIBUTENAME", "")).strip()
        if attribute != type_.upper():
            errors["ATTRIBUTENAME"] = f"Must be '{type_.upper()}'"

        if type_ == "category":
            if str(row.get("PRIMARYCATEGORY", "")).strip() != "TRUE":
                errors["PRIMARYCATEGORY"] = "Must be 'TRUE' (all caps)"

            if str(row.get("CATEGORYSYSTEMTYPE", "")).strip() != "navteq-lcms":
                errors["CATEGORYSYSTEMTYPE"] = "Must be 'navteq-lcms'"

            if str(row.get("PREVIOUSCATEGORYSYSTEMTYPE", "")).strip() != "navteq-lcms":
                errors["PREVIOUSCATEGORYSYSTEMTYPE"] = "Must be 'navteq-lcms'"

            cat_id = row.get("ID", "")
            prev_id = row.get("PREVIOUSID", "")

            if not is_valid_category_id(cat_id):
                errors["ID"] = "Invalid format (###-####-####)"

            if not is_valid_category_id(prev_id):
                errors["PREVIOUSID"] = "Invalid format"
            elif prev_id in protected_categories:
                errors["PREVIOUSID"] = "Protected ID not allowed"

        elif type_ == "status":
            valid_status = {"Open", "Closed", "Temporarily Closed"}
            status = str(row.get("STATUS", "")).strip()
            if status not in valid_status:
                errors["STATUS"] = f"Must be one of {', '.join(valid_status)}"

        elif type_ == "location":
            lat = row.get("LATITUDE", "")
            lon = row.get("LONGITUDE", "")
            if not is_valid_float(lat):
                errors["LATITUDE"] = "Must be a valid number"
            if not is_valid_float(lon):
                errors["LONGITUDE"] = "Must be a valid number"

        if duplicate_placeid_mask[idx]:
            errors["PLACEID_DUPLICATE"] = "Duplicate PLACEID found"

        row_dict = row.to_dict()
        if errors:
            row_dict["Validation_Errors"] = "; ".join(f"{k}: {v}" for k, v in errors.items())
            invalid_rows.append(row_dict)
        else:
            valid_rows.append(row_dict)

    # Create CSVs in memory
    output_type = type_.capitalize()
    valid_csv = io.StringIO()
    pd.DataFrame(valid_rows).to_csv(valid_csv, index=False)
    valid_csv.seek(0)

    invalid_csv = io.StringIO()
    pd.DataFrame(invalid_rows).to_csv(invalid_csv, index=False)
    invalid_csv.seek(0)

    zip_buffer = io.BytesIO()
    with ZipFile(zip_buffer, 'w') as zipf:
        zipf.writestr(f"Valid{output_type}.csv", valid_csv.getvalue())
        zipf.writestr(f"Invalid{output_type}.csv", invalid_csv.getvalue())
    zip_buffer.seek(0)

    return send_file(zip_buffer, mimetype='application/zip', download_name=f"Validation_{output_type}.zip", as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
