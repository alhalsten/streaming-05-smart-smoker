Angela Halsten 9/30/23
# streaming-05-smart-smoker

## Creating the producer
1. Copied code from v3_emitter_of_tasks.py from Module 4
2. Made copies of def stream_message, def prepare_message_from_row, def stream_row to allow each message to be created from each row of data in smoker-temps.csv
3. Set for 3 queues and 3 callbacks.

## Before You Begin

1. Fork this starter repo into your GitHub.
2. Clone your repo down to your machine.
3. View / Command Palette - then Python: Select Interpreter
4. Select your conda environment.

## Execute the Producer

1. Run v1_emitter_of_smoker_tasks.py (say y to monitor RabbitMQ queues)

## Screenshot of producer running
![Alt text](<Screenshot 2023-09-23 193810.png>)

## Create 3 consumers
1. Created the Smoker_consumer from Module 4 consumer
2. Added dequeing to calculate for alerts
3. Set to smoker queue
4. Create FoodA_consumer and FoodB_consumer from Smoker_Consumer
5. Make necessary changes to deque calculations
6. Changed queues
![Alt text](<Screenshot 2023-09-30 152412.png>)