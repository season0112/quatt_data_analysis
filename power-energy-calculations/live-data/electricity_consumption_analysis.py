import pandas as pd
from sklearn.linear_model import Lasso
from sklearn.metrics import mean_squared_error

def preProcessSplitData(data):
    # Correct circulating pump power values
    data['circulatingPumpDutyCycle'] = data['circulatingPumpDutyCycle']*data['getCirculatingPumpRelay']

    # Split the data into input features (X) and target variable (y)
    feature_cols = ['hp1PowerInput', 'circulatingPumpDutyCycle', 'getFanSpeed']
    X_train = train_data[feature_cols]
    y_train = train_data['PowerIn']

    return X_train, y_train

# Load training data from CSV file into a pandas DataFrame
train_data_file = "data/Energy measurements CIC-9368bfef-7eca-5bda-9a90-8d5a4be375c6 - Power consuption-data-2023-03-22 16_16_45.csv"
train_data = pd.read_csv(train_data_file, header=0)

# Pre-process and split data
X_train, y_train = preProcessSplitData(train_data)

# Create a Lasso model with alpha=0.1 (controls the strength of regularization)
lasso = Lasso(alpha=0.1)

# Train the model on the training data
lasso.fit(X_train, y_train)

# Load test data from CSV file into a pandas DataFrame
test_data_file = "data/Energy measurements validation CIC-0f293b7a-4524-5fc7-84b9-66f80a5a6d7c - Power consuption-data-2023-03-22 16_00_42.csv"
test_data = pd.read_csv(test_data_file, header=0)

# Split the data into input features (X) and target variable (y)
X_test, y_test = preProcessSplitData(test_data)

# Evaluate the model on the test data using mean squared error (MSE)
y_pred = lasso.predict(X_test)
mse = mean_squared_error(y_test, y_pred)
print('MSE on test data:', mse)
