from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware
import pickle
import pandas as pd
import os

from database import SessionLocal, Sales, Sentiment

app = FastAPI()

# 🔥 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🔥 LOAD MODELS
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sentiment_model = pickle.load(open(os.path.join(BASE_DIR, "models", "models", "sentiment_model.pkl"), "rb"))
vectorizer = pickle.load(open(os.path.join(BASE_DIR, "models", "models", "tfidf_vectorizer.pkl"), "rb"))
sales_model = pickle.load(open(os.path.join(BASE_DIR, "models", "models", "sales_model.pkl"), "rb"))

# 🔷 INPUT VALIDATION

class Review(BaseModel):
    review: str = Field(
        example="This product is amazing",
        description="Customer review text"
    )

class SalesInput(BaseModel):
    units: float = Field(gt=0, example=5)
    price: float = Field(gt=0, example=100)
    rating: float = Field(ge=1, le=5, example=4.5)

# 🔷 HOME
@app.get("/")
def home():
    return {"message": "SmartBIZ API Running"}

# 🔷 SENTIMENT API
@app.post("/predict-sentiment")
def predict_sentiment(data: Review):

    vector = vectorizer.transform([data.review])
    prediction = sentiment_model.predict(vector)[0]

    db = SessionLocal()

    entry = Sentiment(
        review=data.review,
        sentiment=prediction
    )

    db.add(entry)
    db.commit()
    db.close()

    return {
        "status": "success",
        "message": "Sentiment predicted",
        "data": {
            "sentiment": prediction
        }
    }

# 🔷 SALES API
@app.post("/predict-sales")
def predict_sales(data: SalesInput):

    if data.units <= 0:
        raise HTTPException(status_code=400, detail="Units must be > 0")

    df = pd.DataFrame([{
        "Units Sold": data.units,
        "Unit Price": data.price,
        "Rating": data.rating
    }])

    prediction = sales_model.predict(df)[0]

    db = SessionLocal()

    entry = Sales(
        units=data.units,
        price=data.price,
        rating=data.rating,
        predicted_revenue=float(prediction)
    )

    db.add(entry)
    db.commit()
    db.close()

    return {
        "status": "success",
        "message": "Sales predicted",
        "data": {
            "predicted_revenue": float(prediction)
        }
    }

# 🔷 DASHBOARD API (REAL DATA)
@app.get("/dashboard-data")
def dashboard_data():

    db = SessionLocal()

    sales = db.query(Sales).all()
    sentiments = db.query(Sentiment).all()

    db.close()

    revenue = [s.predicted_revenue for s in sales]

    pos = sum(1 for s in sentiments if s.sentiment == "Positive")
    neg = sum(1 for s in sentiments if s.sentiment == "Negative")
    neu = sum(1 for s in sentiments if s.sentiment == "Neutral")

    return {
        "revenue_over_time": revenue,
        "dates": list(range(len(revenue))),
        "sentiment": {
            "positive": pos,
            "neutral": neu,
            "negative": neg
        }
    }

# 🔷 MODEL PERFORMANCE API
@app.get("/model-performance")
def model_performance():
    return {
        "sentiment_models": {
            "Logistic Regression": 0.85,
            "Naive Bayes": 0.82,
            "Random Forest": 0.88
        },
        "sales_models": {
            "Linear Regression": 0.76,
            "Random Forest": 0.91
        }
    }

