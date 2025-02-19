# CSV Handling App

## How to run the app

Before you start, please add your OpenAI api key to the [.env file](.env)
To run the app you must use the following commands:
```console
sudo docker build -t CONTAINER .
sudo docker run -p 8501:8501 CONTAINER
```

You can access the app under <http://localhost:8501>

## Functionality
You can upload your files. Once you have uploaded a file and selected a table, you can see the whole table and the rows where the table has missing values. Below this you can see the columns with missing values. The suggested value is a value guessed by the LLM, but the user can choose to select a value themselves. The value selection is dynamically based on the data type of the column.
When the user selects the `Save corrections` button, the values for the missing columns are loaded into a SQLite database and a download button appears for the user to download the corrected CSV.