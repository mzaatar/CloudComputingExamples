using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace AWSDemos.DynamoDB
{
    internal class Program
    {
        private static readonly AmazonDynamoDBClient Client = new AmazonDynamoDBClient();
        private const string TablbName = "Customers";

        public static void Main(string[] args)
        {
            // creating the DynamoDB table
            var resultCreation = CreateDynamoDb(TablbName);
            if (resultCreation == 0)
            {
                Console.WriteLine("Table created");
                Console.WriteLine("-- Waiting to crate the table--");
            }
            else if (resultCreation == 1)
                Console.WriteLine("Table cannot be created ");
            else
                Console.WriteLine("Cannot create the table because the table exists");

            // add/update items
            for (int i = 0; i < 5; i++)
            {
                AddItems(TablbName, i);
            }

            //Update item 
            UpdateMultipleAttributes(TablbName, 1);
            Console.WriteLine("----");

            // query items
            Getitem(TablbName, 1);
            Console.WriteLine("----");

            // delete items
            DeleteItem(TablbName, 3);
            Console.WriteLine("----");

            // delete table
            DeleteTable(TablbName);
            Console.WriteLine("----");

            Console.Read();
        }

        private static void DeleteItem(string tableName, int itemNumber)
        {
            try
            {
                DeleteItemRequest request = new DeleteItemRequest
                {
                    TableName = tableName,
                    Key = new Dictionary<string, AttributeValue>()
                    {
                        { "Id", new AttributeValue { N = itemNumber.ToString() } }
                        ,{"CustomerName", new AttributeValue {S = "CustomerName #" + itemNumber}}
                    }
                };
                DeleteItemResponse response = Client.DeleteItem(request);
                if (response.HttpStatusCode == HttpStatusCode.OK)
                    Console.WriteLine("Item #" + itemNumber + " was deleted from table " + tableName);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error deleting item #" + itemNumber + " from table " + tableName);
                Console.WriteLine("--------------------------------------------------");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.InnerException.Message);
                Console.WriteLine("--------------------------------------------------");
            }
        }

        /// <summary>
        /// 0 means created
        /// 1 means there is a n error
        /// 2 means it already exists
        /// </summary>
        /// <param name="tablebName"></param>
        /// <returns></returns>
        private static int CreateDynamoDb(string tableName)
        {
            // check if the table exits
            if (Client.ListTables().TableNames.Any(s => s.Contains(tableName)))
            {
                return 2;
            }


            var request = new CreateTableRequest
            {
                TableName = tableName,

                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition
                    {
                        AttributeName = "Id",
                        AttributeType = "N"
                    }
                    ,
                    new AttributeDefinition
                    {
                        AttributeName = "CustomerName",
                        AttributeType = "S"
                    }
                },
                KeySchema = new List<KeySchemaElement>()
                {
                    new KeySchemaElement
                    {
                        AttributeName = "Id",
                        KeyType = "HASH" //Partition key
                    }
                    ,
                    new KeySchemaElement
                    {
                        AttributeName = "CustomerName",
                        KeyType = "RANGE" //Sort key
                    }
                }
                ,
                ProvisionedThroughput = new ProvisionedThroughput()
                {
                    ReadCapacityUnits = 5,
                    WriteCapacityUnits = 5
                }
            };

            var response = Client.CreateTable(request);

            if (response.HttpStatusCode == HttpStatusCode.OK)
                return 0;
            return 1;
        }

        private static void AddItems(string tableName, int itemNumber)
        {
            try
            {
                PutItemRequest request = new PutItemRequest
                {
                    TableName = tableName,
                    Item = new Dictionary<string, AttributeValue>()
                    {
                        {"Id", new AttributeValue {N = itemNumber.ToString()}},
                        {"CustomerName", new AttributeValue {S = "CustomerName #" + itemNumber}},
                        {"Address", new AttributeValue {SS = new List<string> {"7 Eleven street", "Perth, WA 6000"}}},
                        {"DoB", new AttributeValue {N = "1984"}},
                        {"IsActive", new AttributeValue {BOOL = false}}
                    }
                };
                PutItemResponse response = Client.PutItem(request);
                if (response.HttpStatusCode == HttpStatusCode.OK)
                    Console.WriteLine("Item #" + itemNumber + " was Added to table " + tableName);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error adding item #" + itemNumber + " to table " + tableName);
                Console.WriteLine("--------------------------------------------------");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.InnerException.Message);
                Console.WriteLine("--------------------------------------------------");
            }
        }

        private static void Getitem(string tableName, int itemNumber)
        {
            try
            {
                var request = new GetItemRequest
                {
                    TableName = tableName,
                    Key = new Dictionary<string, AttributeValue>()
                    {
                        {"Id", new AttributeValue {N = itemNumber.ToString()}},
                        {"CustomerName", new AttributeValue {S = "CustomerName #" + itemNumber}}
                    },
                    ProjectionExpression = "Id, CustomerName, Address, DoB",
                    ConsistentRead = true
                };

                GetItemResponse response = Client.GetItem(request);
                if (response.HttpStatusCode == HttpStatusCode.OK)
                {
                    Console.WriteLine("Item #" + itemNumber + " was retrieved" + response.Item);
                    PrintItem(response.Item);
                }
                else
                {
                    Console.WriteLine(response.HttpStatusCode + " " + response.ResponseMetadata);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error retrieving item #" + itemNumber + " from table " + tableName);
                Console.WriteLine("--------------------------------------------------");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.InnerException.Message);
                Console.WriteLine("--------------------------------------------------");
            }
        }

        private static void UpdateMultipleAttributes(string tableName, int itemNumber)
        {
            try
            {
                var request = new UpdateItemRequest
                {
                    Key = new Dictionary<string, AttributeValue>()
                    {
                        {"Id", new AttributeValue {N = itemNumber.ToString()}},
                        {"CustomerName", new AttributeValue {S = "CustomerName #" + itemNumber}}
                    },

                    ExpressionAttributeNames = new Dictionary<string, string>()
                    {
                        {"#TI", "Title"},
                        {"#DB", "DoB"},
                        {"#I", "IsActive"}
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
                    {
                        {":i", new AttributeValue {BOOL = true}},
                        {":new",new AttributeValue {S = "new value"}}
                    },

                    //Note : take care that Add accepts only int and lists !!!

                    UpdateExpression = "SET #TI  = :new , #I = :i REMOVE #DB",

                    TableName = tableName,
                    ReturnValues = "ALL_NEW" // Give me all attributes of the updated item.
                };
                var response = Client.UpdateItem(request);

                // Check the response.
                var attributeList = response.Attributes; // attribute list in the response.
                
                // print attributeList.
                Console.WriteLine("Printing item after multiple attribute update ............");
                PrintItem(attributeList);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error updating item #" + itemNumber + " from table " + tableName);
                Console.WriteLine("----------------------------");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.InnerException.Message);
                Console.WriteLine("----------------------------");
            }
        }

        private static void DeleteTable(string tableName)
        {
            DeleteTableRequest request = new DeleteTableRequest(TablbName);
            DeleteTableResponse response = Client.DeleteTable(request);
            if (response.HttpStatusCode == HttpStatusCode.OK)
                Console.WriteLine(TablbName + " table deleted");
            Console.WriteLine(response.HttpStatusCode + " " + response.ResponseMetadata);
        }

        /// <summary>
        /// source : http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LowLevelDotNetItemsExample.html
        /// </summary>
        /// <param name="attributeList"></param>
        private static void PrintItem(Dictionary<string, AttributeValue> attributeList)
        {
            Console.WriteLine("************************************************");
            foreach (KeyValuePair<string, AttributeValue> kvp in attributeList)
            {
                string attributeName = kvp.Key;
                AttributeValue value = kvp.Value;

                Console.WriteLine(
                    attributeName + " " +
                    (value.S == null ? "" : "S=[" + value.S + "]") +
                    (value.N == null ? "" : "N=[" + value.N + "]") +
                    (value.SS == null ? "" : "SS=[" + string.Join(",", value.SS.ToArray()) + "]") +
                    (value.NS == null ? "" : "NS=[" + string.Join(",", value.NS.ToArray()) + "]")
                    );
            }
            Console.WriteLine("************************************************");
        }
    }
}