package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type OutputType struct {
	Zip       int      `json:"zip"`
	CenterLat float64  `json:"centerLat"`
	CenterLng float64  `json:"centerLng"`
	Outline   []LatLng `json:"outline"`
}

type LatLng struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

func main() {
	key := os.Getenv("AWS_KEY")
	secret := os.Getenv("AWS_SECRET")

	// Create a new AWS session with explicit credentials
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-2"),
		Credentials: credentials.NewStaticCredentials(key, secret, ""),
	})
	if err != nil {
		fmt.Println("Error creating session:", err)
		return
	}
	svc := dynamodb.New(sess)

	// Open the file for reading
	file, err := os.Open("output.txt")
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("File opened\n")
	}
	defer file.Close()

	// Open the error file for reading
	errorFile, err := os.Open("errors.csv")
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Printf("Error file opened\n")
	}
	defer errorFile.Close()

	// Create error file
	newErrorFile, err := os.Create("errors.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer errorFile.Close()
	newErrorFile.WriteString("Counter,Zip,Error\n")

	// Create a scanner to read the file line by line
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024*128)
	scanner.Buffer(buf, bufio.MaxScanTokenSize)

	// Make a counter to track progress
	var counter = 0

	// Define a command-line flag for the cursor
	cursorPtr := flag.Int("cursor", 0, "initial cursor position")
	flag.Parse()

	// Use the command-line flag as the initial value of the cursor
	cursor := *cursorPtr

	// Create a map to store the zip codes from errors.csv
	zipCodes := make(map[string]bool)

	// Read the zip codes from errors.csv
	errorScanner := bufio.NewScanner(errorFile)
	for errorScanner.Scan() {
		fields := strings.Split(errorScanner.Text(), ",")
		if len(fields) >= 2 {
			zipCodes[fields[1]] = true
		}
	}

	// Iterate over each line in the file
	for scanner.Scan() {
		// Parse the line into a struct
		var obj OutputType
		err := json.Unmarshal(scanner.Bytes(), &obj)
		if err != nil {
			fmt.Println("Error unmarshaling")
			log.Fatal(err)
		}

		// Convert the []LatLng slice to a JSON string
		outlineBytes, err := json.Marshal(obj.Outline)
		if err != nil {
			fmt.Println("Error marshaling")
			log.Fatal(err)
		}
		outlineStr := string(outlineBytes)

		if counter < cursor {
			fmt.Printf("Skipping %d\n", counter)
		} else {
			fmt.Printf("Processing %d\n", counter)

			// Check if the zip code is in the errors.csv file
			if zipCodes[strconv.Itoa(obj.Zip)] {
				// Check if the item exists in the table
				getItemInput := &dynamodb.GetItemInput{
					TableName: aws.String("geo_zip"),
					Key: map[string]*dynamodb.AttributeValue{
						"Zip": {
							N: aws.String(strconv.Itoa(obj.Zip)),
						},
					},
				}
				_, err = svc.GetItem(getItemInput)

				if err != nil {
					// If the item does not exist, insert a new item
					putItemInput := &dynamodb.PutItemInput{
						TableName: aws.String("geo_zip"),
						Item: map[string]*dynamodb.AttributeValue{
							"Zip": {
								N: aws.String(strconv.Itoa(obj.Zip)),
							},
							"Center": {
								L: []*dynamodb.AttributeValue{
									{N: aws.String(strconv.FormatFloat(obj.CenterLat, 'f', -1, 64))},
									{N: aws.String(strconv.FormatFloat(obj.CenterLng, 'f', -1, 64))},
								},
							},
							"Outline": {
								S: aws.String(outlineStr),
							},
						},
					}
					_, err = svc.PutItem(putItemInput)
					if err != nil {
						fmt.Println("Error inserting item:", err)
					}
				} else {
					// If the item exists, update the item
					updateItemInput := &dynamodb.UpdateItemInput{
						TableName: aws.String("geo_zip"),
						Key: map[string]*dynamodb.AttributeValue{
							"Zip": {
								N: aws.String(strconv.Itoa(obj.Zip)),
							},
						},
						UpdateExpression: aws.String("SET #center = :center, #outline = :outline"),
						ExpressionAttributeNames: map[string]*string{
							"#center":  aws.String("Center"),
							"#outline": aws.String("Outline"),
						},
						ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
							":center": {
								L: []*dynamodb.AttributeValue{
									{N: aws.String(strconv.FormatFloat(obj.CenterLat, 'f', -1, 64))},
									{N: aws.String(strconv.FormatFloat(obj.CenterLng, 'f', -1, 64))},
								},
							},
							":outline": {
								S: aws.String(outlineStr),
							},
						},
					}
					_, err = svc.UpdateItem(updateItemInput)
					if err != nil {
						fmt.Println("Error updating item:", err)
						newErrorFile.WriteString(fmt.Sprintf("%d,%d,%s\n", counter, obj.Zip, strings.ReplaceAll(err.Error(), "\n", "")))
					}
				}
			} else {
				// If the zip code is not in the errors.csv file, insert a new item
				putItemInput := &dynamodb.PutItemInput{
					TableName: aws.String("geo_zip"),
					Item: map[string]*dynamodb.AttributeValue{
						"Zip": {
							N: aws.String(strconv.Itoa(obj.Zip)),
						},
						"Center": {
							L: []*dynamodb.AttributeValue{
								{N: aws.String(strconv.FormatFloat(obj.CenterLat, 'f', -1, 64))},
								{N: aws.String(strconv.FormatFloat(obj.CenterLng, 'f', -1, 64))},
							},
						},
						"Outline": {
							S: aws.String(outlineStr),
						},
					},
				}
				_, err = svc.PutItem(putItemInput)
				if err != nil {
					fmt.Println("Error inserting item:", err)
					newErrorFile.WriteString(fmt.Sprintf("%d,%d,%s\n", counter, obj.Zip, strings.ReplaceAll(err.Error(), "\n", "")))
				}
			}
		}

		counter++
	}

	fmt.Printf("Output written to DynamoDB\n")
}
