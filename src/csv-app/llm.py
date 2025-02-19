import pandas as pd
import os
import re
import json
import asyncio
from dotenv import load_dotenv
from typing import Union
from openai import OpenAI, AsyncOpenAI


class LLMAgent:
    """
    Basic class for the LLM Agent

    Parameters
    ----------
    prompt_folder : str
        The parent folder of the diffent prompt subfolders
    model_name : str
        The name of the model
    api_key : str
        The API key for the OpenAI API
    port : int
        The port of the OpenAI API

    Methods
    -------
    read_prompt(prompt_subfolder, prompt_name, file_ending)
        Reads a prompt from a file
    send_prompt(system_prompt, user_prompts, temperature)
        Sends prompt and returns response
    """

    def __init__(self, model_name: str = None, temperature: float = None):
        load_dotenv()
        self.api_key = os.getenv("OPENAI_API_KEY")
        if model_name is None:
            self.model_name = os.getenv("MODEL_NAME")
        else:
            self.model_name = model_name
        if temperature is None:
            self.temperature = float(os.getenv("TEMPERATURE"))
        else:
            self.temperature = temperature

        self.model_name = (
            model_name
            if model_name is not None
            else os.getenv("MODEL_NAME")
        )
        self.temperature = (
            temperature
            if temperature is not None
            else float(os.getenv("TEMPERATURE", 0.7))
        )
        self.prompt_folder = os.path.join(
            os.path.dirname(
                os.path.dirname(os.path.abspath(__file__))
            ), "prompts"
        )
        self.suggesting_prompt = self.read_prompt(
            os.getenv("SUGGESTION_PROMPT")
        )
        self.client = AsyncOpenAI(api_key=self.api_key)

    def read_prompt(
        self, prompt_file_name: str
    ) -> str:
        prompt_path = os.path.join(
            self.prompt_folder, prompt_file_name
        )

        with open(prompt_path, "r") as prompt:
            return prompt.read()

    def prepare_prompt(
        self,
        summary_stats_json: str,
        corr_matrix_json: str,
        df_row: pd.DataFrame,
        column_missing: str,
    ) -> str:
        """
        Takes the summary statistics, correlation matrix, the row with
        the missing value and the column name and prepares the prompt for the LLM

        Parameters
        ----------
        summary_stats_json : str
            The summary statistics in JSON format
        corr_matrix_json : str
            The correlation matrix in JSON format
        df_row : pd.DataFrame
            The row with the missing value
        column_missing : str
            The column name of the missing value

        Returns
        -------
        str
            The prepared user prompt
        """
        columns_nan = [
            col for col in df_row.columns if pd.isnull(df_row[col].values[0])
        ]
        temp_row_no_nan = df_row[df_row.columns.difference(columns_nan)]
        temp_row_no_nan = temp_row_no_nan.iloc[0].to_json()
        column_dtype = df_row[column_missing].dtype

        user_prompt = f"""
        <row>{temp_row_no_nan}</row>
        <column_name>>{column_missing}</<column_name>
        <column_dtype>>{column_dtype}</<column_dtype>
        <summary_statistics>{summary_stats_json}</summary_statistics>
        <correlation_matrix>{corr_matrix_json}</correlation_matrix>
        """

        return user_prompt

    async def send_prompt_async(
        self,
        user_prompt: str
    ) -> str:
        """
        Sends prompt and returns response asynchronously

        Parameters
        ----------
        user_prompt : str
            The user prompt

        Returns
        -------
        str
            The response
        """
        messages = [{"role": "developer", "content": self.suggesting_prompt}]
        messages.append({"role": "user", "content": user_prompt})

        response = await self.client.chat.completions.create(
            model=self.model_name,
            messages=messages,
            temperature=self.temperature
        )
        return response.choices[0].message.content

    async def get_response(
        self,
        user_prompt: str,
        index: int,
        column_missing: str
    ):
        """
        Sends prompt and returns response asynchronously

        Parameters
        ----------
        user_prompt : str
            The user prompt
        index : int
            The index of the row
        column_missing : str
            The column name of the missing value

        Returns
        -------
        str
            The response
        """
        response = await self.send_prompt_async(user_prompt)
        return {
            "index": index,
            "column_missing": column_missing,
            "response": response
        }

    async def send_missing_values_to_llm(
        self,
        df_original: pd.DataFrame,
        df_error: pd.DataFrame
    ):
        summary_stats_json = df_original.describe().to_json()
        corr_matrix_json = (
            df_original.select_dtypes(include='number').corr().to_json()
        )
        tasks = []
        async with asyncio.TaskGroup() as tg:
            for index, _ in df_error.iterrows():
                temp_row = df_error.iloc[[index]]
                columns_nan = [
                    col for col in temp_row.columns if
                    pd.isnull(temp_row[col].values[0])
                ]

                for column_missing in columns_nan:
                    prepared_prompt = self.prepare_prompt(
                        summary_stats_json=summary_stats_json,
                        corr_matrix_json=corr_matrix_json,
                        df_row=temp_row,
                        column_missing=column_missing
                    )
                    task = tg.create_task(
                        self.get_response(
                            prepared_prompt, index, column_missing
                        )
                    )
                    tasks.append(task)

        return [task.result() for task in tasks]

    def _extract_json_from_response(
        self,
        response: str
    ) -> dict:
        """
        Extracts the JSON from the response.

        Parameters
        ----------
        response : str
            The response to extract from.

        Returns
        -------
        dict
            The extracted JSON.
        """
        try:
            return json.loads(
                re.search(r"```json\s([\s\S]*?)```", response).group(1)
            )
        except AttributeError:
            print(f"""
            Json could not be found for response:
            {response}
            """)
            raise AttributeError

    def gather_respones(
        self,
        response_list: list[dict[str, str, str]]
    ) -> dict[str, dict[str, str]]:
        """
        Gathers the responses into a dictionary

        Parameters
        ----------
        response_list : list[dict[str, str, str]]
            The list of responses

        Returns
        -------
        dict[str, dict[str, str]]
            The dictionary with the responses
        """
        responses_dict = {}
        for item in response_list:
            index = item["index"]
            column_missing = item["column_missing"]
            response = self._extract_json_from_response(
                item["response"]
            )["value"]
            if index not in responses_dict:
                responses_dict[index] = {}
            responses_dict[index][column_missing] = response

        return responses_dict