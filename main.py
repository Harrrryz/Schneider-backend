from fastapi import FastAPI
import ormar


app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


# c,f,i 筛选
