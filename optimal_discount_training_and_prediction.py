import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report
from ml_model_specification import trainData, testData, binaryClassification

class OptimalDiscountTrainingAndPrediction:
    def __init__(self, training_data_set, prediction_data_set, train_test_split_ratio = 1, EPOCHS = 50, BATCH_SIZE = 64, LEARNING_RATE = 0.001):
        self.training_data_set = training_data_set
        self.prediction_data_set = prediction_data_set
        self.train_test_split_ratio = train_test_split_ratio
        self.EPOCHS = EPOCHS
        self.BATCH_SIZE = BATCH_SIZE
        self.LEARNING_RATE = LEARNING_RATE
        
    @staticmethod
    def binary_acc(y_pred, y_test):
        y_pred_tag = torch.round(torch.sigmoid(y_pred))

        correct_results_sum = (y_pred_tag == y_test).sum().float()
        acc = correct_results_sum/y_test.shape[0]
        acc = torch.round(acc * 100)

        return acc
    
    def train_and_predict(self): 
        
        train_set = self.training_data_set
        train_set_labels = train_set.pop('label')
        sc = StandardScaler()
        
        x = sc.fit_transform(train_set)
        y = train_set_labels
        
        beta = sc.fit_transform(self.prediction_data_set)
        
        print("shape of x: {}\nshape of y: {}".format(x.shape,y.shape))
        print("shape of prediction: {}".format(beta.shape))
        
        train_data = trainData(torch.FloatTensor(x), torch.FloatTensor(y.values))
        test_data = testData(torch.FloatTensor(beta))
        
        train_loader = DataLoader(dataset=train_data, batch_size=self.BATCH_SIZE, shuffle=True)
        test_loader = DataLoader(dataset=test_data, batch_size=1)
        
        device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        # print(device)

        model = binaryClassification()
        model.to(device)
        # print(model)

        # 11 is speicifc to hydronize data 
        # it is the weight for positive labels in an attempt to solve the skewness issues
        # not dynamic yet 
        criterion = nn.BCEWithLogitsLoss(pos_weight = torch.tensor([11]) )
        optimizer = optim.Adam(model.parameters(), lr=self.LEARNING_RATE)

        model.train()
        for e in range(1, self.EPOCHS+1):
            epoch_loss = 0
            epoch_acc = 0
            for X_batch, y_batch in train_loader:
                X_batch, y_batch = X_batch.to(device), y_batch.to(device)
                optimizer.zero_grad()

                y_pred = model(X_batch)

                loss = criterion(y_pred, y_batch.unsqueeze(1))
                acc = self.binary_acc(y_pred, y_batch.unsqueeze(1))

                loss.backward()
                optimizer.step()

                epoch_loss += loss.item()
                epoch_acc += acc.item()

            print(f'Epoch {e+0:03}: | Loss: {epoch_loss/len(train_loader):.5f} | Acc: {epoch_acc/len(train_loader):.3f}')
        

        y_pred_list = []
        y_sigmoid_val = []
        model.eval()
        with torch.no_grad():
            for X_batch in test_loader:
                X_batch = X_batch.to(device)
                y_test_pred = model(X_batch)
                y_test_pred = torch.sigmoid(y_test_pred)

                y_sigmoid_val.append(y_test_pred.numpy()[0][0])

                y_pred_tag = torch.round(y_test_pred)
                y_pred_list.append(y_pred_tag.cpu().numpy())
        
        
        y_pred_list = [a.squeeze().tolist() for a in y_pred_list]
        
        prob = pd.DataFrame({'sigmoid': y_sigmoid_val, 'prediction': y_pred_list}, columns=['sigmoid', 'prediction'])

        return prob