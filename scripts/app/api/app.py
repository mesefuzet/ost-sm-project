# This is a test comment for GitHub validation,test
from flask import Flask, render_template, request, jsonify
from utilities import process

app = Flask(__name__)

# Define the route for the home page
@app.route('/')
def index():
    return render_template('index.html')

@app.route("/predict", methods=["POST"])
def predict():
    if request.method == 'POST':
        # Handle the uploaded file and process it
        file = request.files['file']
        # Process the file here and generate results
        results = process(file)
        return render_template('index.html', results=results)

if __name__ == "__main__":
    app.run(debug=True, port=5000)