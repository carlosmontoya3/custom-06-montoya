# custom-06-montoya

This project ingests live data from a local file and processes it using a custom Kafka consumer. The consumer reads real-time messages, processes them, and stores insights in a SQLite database. Additionally, sentiment analysis is performed on each message to track trends over time.

## VS Code Extensions

For best performance and code management, ensure the following extensions are installed:

- **Black Formatter** by Microsoft (for automatic code formatting)
- **Markdown All in One** by Yu Zhang (for better Markdown support)
- **PowerShell** by Microsoft (for running scripts on Windows)
- **Pylance** by Microsoft (for Python code intelligence)
- **Python** by Microsoft (for running Python scripts)
- **Python Debugger** by Microsoft (for debugging Python code)
- **Ruff** by Astral Software (Linter for enforcing Python style)
- **SQLite Viewer** by Florian Klampfer (for viewing SQLite databases)
- **WSL** by Microsoft (for running Linux tools on Windows)


## Task 1. Use Tools from Module 1 and 2

Before starting, ensure you have completed the setup tasks in <https://github.com/denisecase/buzzline-01-case> and <https://github.com/denisecase/buzzline-02-case> first. 

**Important:** Python **3.11** is required for this project.  
Additionally, ensure you have the correct **Java JDK** and other dependencies installed.

## Task 2. Copy This Example Project and Rename

Once the tools are installed, copy/fork this project into your GitHub account
and create your own version of this project to run and experiment with. 
Follow the instructions in [FORK-THIS-REPO.md](https://github.com/denisecase/buzzline-01-case/docs/FORK-THIS-REPO.md).

OR: For more practice, add these example scripts or features to your earlier project. 
You'll want to check requirements.txt, .env, and the consumers, producers, and util folders. 
Use your README.md to record your workflow and commands. 
    

## Task 3. Manage Local Project Virtual Environment

Follow the instructions in [MANAGE-VENV.md] to:
1. **Create** your virtual environment (`.venv`).
2. **Activate** `.venv` (Windows: `venv\Scripts\activate`, Mac/Linux: `source venv/bin/activate`).
3. **Install** the required dependencies using `requirements.txt` (`pip install -r requirements.txt`).

## Task 4. Start Zookeeper and Kafka (Takes 2 Terminals)

If Zookeeper and Kafka are not already running, you'll need to restart them.
See instructions at [SETUP-KAFKA.md] to:

1. Start Zookeeper Service ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-7-start-zookeeper-service-terminal-1))
2. Start Kafka Service ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-8-start-kafka-terminal-2))

---

## Task 5. Start a New Streaming Application

This will take two more terminals:

1. One to run the producer which writes messages. 
2. Another to run the consumer which reads messages, processes them, and writes them to a data store. 

### Producer (Terminal 3) 

Start the producer to generate the messages. 
The existing producer writes messages to a live data file in the data folder.
If Zookeeper and Kafka services are running, it will try to write them to a Kafka topic as well.
For configuration details, see the .env file. 

In VS Code, open a NEW terminal.
Use the commands below to activate .venv, and start the producer. 

Windows:

```shell
.venv\Scripts\activate
py -m producers.producer_montoya
```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m producers.producer_montoya
```

The producer will still work if Kafka is not available.

### Consumer (Terminal 4) - Two Options

Start an associated consumer. 
You have two options. 
1. Start the consumer that reads from the live data file.
2. OR Start the consumer that reads from the Kafka topic.

In VS Code, open a NEW terminal in your root project folder. 
Use the commands below to activate .venv, and start the consumer. 

Windows:
```shell
.venv\Scripts\activate
py -m consumers.consumer_montoya
```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m consumers.consumer_montoya
```

---

## Review the Project Code

Review the requirements.txt file. 
- What - if any - new requirements do we need for this project?
- Note that requirements.txt now lists both kafka-python and six. 
- What are some common dependencies as we incorporate data stores into our streaming pipelines?

Review the .env file with the environment variables.
- Why is it helpful to put some settings in a text file?
- As we add database access and passwords, we start to keep two versions: 
   - .evn 
   - .env.example
 - Read the notes in those files - which one is typically NOT added to source control?
 - How do we ignore a file so it doesn't get published in GitHub (hint: .gitignore)

Review the .gitignore file.
- What new entry has been added?

Review the code for the producer and the two consumers.
 - Understand how the information is generated by the producer.
 - Understand how the different consumers read, process, and store information in a data store?

Compare the consumer that reads from a live data file and the consumer that reads from a Kafka topic.
- Which functions are the same for both?
- Which parts are different?

What files are in the utils folder? 
- Why bother breaking functions out into utility modules?
- Would similar streaming projects be likely to take advantage of any of these files?

What files are in the producers folder?
- How do these compare to earlier projects?
- What has been changed?
- What has stayed the same?

What files are in the consumers folder?
- This is where the processing and storage takes place.
- Why did we make a separate file for reading from the live data file vs reading from the Kafka file?
- What functions are in each? 
- Are any of the functions duplicated? 
- Can you refactor the project so we could write a duplicated function just once and reuse it? 
- What functions are in the sqlite script?
- What functions might be needed to initialize a different kind of data store?
- What functions might be needed to insert a message into a different kind of data store?

---

## Explorations

- Did you run the kafka consumer or the live file consumer? Why?
- Can you use the examples to add a database to your own streaming applications? 
- What parts are most interesting to you?
- What parts are most challenging? 

---

## Later Work Sessions
When resuming work on this project:
1. Open the folder in VS Code. 
2. Open a terminal and start the Zookeeper service. If Windows, remember to start wsl. 
3. Open a terminal and start the Kafka service. If Windows, remember to start wsl. 
4. Open a terminal to start the producer. Remember to activate your local project virtual environment (.env).
5. Open a terminal to start the consumer. Remember to activate your local project virtual environment (.env).

## Save Space
To save disk space, you can delete the .venv folder when not actively working on this project.
You can always recreate it, activate it, and reinstall the necessary packages later. 
Managing Python virtual environments is a valuable skill. 

## License
This project is licensed under the MIT License as an example project. 
You are encouraged to fork, copy, explore, and modify the code as you like. 
See the [LICENSE](LICENSE.txt) file for more.
