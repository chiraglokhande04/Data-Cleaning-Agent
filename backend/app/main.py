from fastapi import FastAPI
# from app.routes import users, items

app = FastAPI()


# app.include_router(users.router)
# app.include_router(items.router)

@app.get("/")
def home():
    return {"message": "FastAPI is working 🚀"}


#To run the server
# uvicorn app.main:app --reload
