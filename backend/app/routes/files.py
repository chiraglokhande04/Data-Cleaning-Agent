# app/routes/files.py
import pandas as pd
from app.models.dataset_metadata import DatasetMetadata, ColumnMeta, Issue, Transformation, ProvenanceEvent

@router.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(await file.read())
        tmp_path = tmp.name

    df = pd.read_csv(tmp_path)

    # Extract schema metadata
    schema = {
        col: ColumnMeta(
            name=col,
            dtype=str(df[col].dtype),
            missing_count=int(df[col].isna().sum()),
            unique_count=int(df[col].nunique()),
            example_values=df[col].dropna().astype(str).unique()[:3].tolist()
        )
        for col in df.columns
    }

    preview = df.head(5).to_dict(orient="records")

    # Upload to Cloudinary
    upload_result = cloudinary.uploader.upload(tmp_path, resource_type="raw", folder="data_cleaning_agent")

    metadata = DatasetMetadata(
        filename=file.filename,
        cloudinary_url=upload_result["secure_url"],
        size=os.path.getsize(tmp_path),
        row_count=len(df),
        preview=preview,
        schema=schema,
        issues=[],
        transformations=[],
        provenance=[ProvenanceEvent(actor="System", action="upload")]
    )

    await files_collection.insert_one(metadata.dict())
    os.remove(tmp_path)

    return {"message": "File uploaded", "dataset": metadata}
