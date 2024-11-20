import torch.nn as nn
import torch.nn.functional as F

from flame.patterns.star import FlameSDK


async def train(flame: FlameSDK):
    data = ''  # get from kong
    while not flame.converged():
        params = await flame.get_params()
        weights = await flame.get_weights()
        model = Net(**params, weights=weights, epoch=flame.get_num_epochs())
        model.train()
        for batch_idx, (data, target) in enumerate(flame.get_data()):

            output = model(data)
            loss = F.nll_loss(output, target)
            loss.backward()
            if batch_idx % 10 == 0:
                flame.log_metric("loss", loss.item())
                flame.log_metric("accuracy", 100. * batch_idx / len(flame.get_data_loader()))
        flame = flame.send_weights(model)
        model = await flame.get_model()

        flame.log_metric("accuracy", 100. * batch_idx / len(flame.get_data_loader()))
        flame.log_metric("loss", loss.item())


class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.conf = self.conv1 = nn.Conv2d(1, 32, 3, 1)
        self.fc = nn.Linear(9216, 10)

    def forward(self, x):
        x = self.conv1(x)
        x = F.relu(x)
        x = self.fc(x)
        return F.log_softmax(x, dim=1)

'''

epoch 0 model - (1 2 3) weights

node 0 (1 2 3) -> gradients -> optimizer -> weights: (0 2 3), loss: 0.5
node 1 (1 2 3) -> gradients -> optimizer -> weights: (0.5 2 2.75), loss: 0.25

aggregator (1 2 3):
-> weights: agg_weights((0 2 3), (0.5 2 2.75)) -> model new_weights (0.25 2 2.875)
   |_-> check weight change for convergence
-> loss: check loss change for convergence 



epoch 1 model - (0.25 2 2.875) new weights

...

'''
