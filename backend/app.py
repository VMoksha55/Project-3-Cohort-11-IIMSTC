from flask import Flask, request, jsonify, render_template, send_from_directory
import pickle
import os
import pandas as pd

app = Flask(__name__)

# 🔷 Load models - path to parent folder's models
MODEL_PATH = os.path.join(os.path.dirname(__file__), '..', 'models', 'models')
sentiment_model = pickle.load(open(os.path.join(MODEL_PATH, 'sentiment_model.pkl'), 'rb'))
tfidf_vectorizer = pickle.load(open(os.path.join(MODEL_PATH, 'tfidf_vectorizer.pkl'), 'rb'))
sales_model = pickle.load(open(os.path.join(MODEL_PATH, 'sales_model.pkl'), 'rb'))

# 🔷 FRONTEND ROUTES
@app.route('/')
def home():
    return render_template('index.html')

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory(os.path.join(os.path.dirname(__file__), '..', 'frontend'), filename)

# 🔷 COMBINED ANALYZE API (for frontend compatibility)
@app.route('/analyze', methods=['POST'])
def analyze():
    data = request.get_json()
    text = data.get('text', '')
    price = float(data.get('price', 0))
    
    # Sentiment
    vector = tfidf_vectorizer.transform([text])
    sentiment = sentiment_model.predict(vector)[0]
    confidence = sentiment_model.predict_proba(vector).max()
    
    # Sales
    from datetime import datetime
    now = datetime.now()
    df = pd.DataFrame([{
        'Unit Price': price,
        'year': now.year,
        'month': now.month,
        'day': now.day,
        'Product Category_Books': 0,
        'Product Category_Clothing': 0,
        'Product Category_Electronics': 0,
        'Product Category_Home Appliances': 0,
        'Product Category_Sports': 0,
        'Region_Europe': 0,
        'Region_North America': 0,
        'Payment Method_Debit Card': 0,
        'Payment Method_PayPal': 0
    }])
    prediction = sales_model.predict(df)[0]
    
    return jsonify({
        'sentiment': sentiment,
        'confidence': round(confidence, 2),
        'sales_prediction': round(prediction, 2)
    })

# 🔷 SENTIMENT API
@app.route('/predict-sentiment', methods=['POST'])
def predict_sentiment():
    data = request.get_json()
    review = data['review']

    vector = tfidf_vectorizer.transform([review])
    prediction = sentiment_model.predict(vector)[0]

    return jsonify({'sentiment': prediction})

# 🔷 SALES API
@app.route('/predict-sales', methods=['POST'])
def predict_sales():
    data = request.get_json()
    from datetime import datetime
    now = datetime.now()

    # Create feature vector with all 13 features the model expects
    df = pd.DataFrame([{
        'Unit Price': float(data.get('unit_price', 0)),
        'year': now.year,
        'month': now.month,
        'day': now.day,
        'Product Category_Books': 0,
        'Product Category_Clothing': 0,
        'Product Category_Electronics': 0,
        'Product Category_Home Appliances': 0,
        'Product Category_Sports': 0,
        'Region_Europe': 0,
        'Region_North America': 0,
        'Payment Method_Debit Card': 0,
        'Payment Method_PayPal': 0
    }])

    prediction = sales_model.predict(df)[0]

    return jsonify({'predicted_revenue': round(prediction, 2)})

# 🔷 RUN SERVER
if __name__ == '__main__':
    app.run(debug=True)