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
	"github.com/joho/godotenv"
)

type InputType struct {
	Zip       int      `json:"zip"`
	CenterLat float64  `json:"centerLat"`
	CenterLng float64  `json:"centerLng"`
	Outline   []LatLng `json:"outline"`
}

type LatLng struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

type OutputType struct {
	Zip     int          `json:"zip"`
	Center  [2]float64   `json:"center"`
	Outline [][2]float64 `json:"outline"`
}

func main() {
	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	key := os.Getenv("AWS_KEY")
	secret := os.Getenv("AWS_SECRET")

	// Create a new AWS session with explicit credentials
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewStaticCredentials(key, secret, ""),
	})
	if err != nil {
		fmt.Println("Error creating session:", err)
		return
	} else {
		fmt.Printf("Session created\n")
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
	// errorFile, err := os.Open("errors.csv")
	// if err != nil {
	// 	log.Fatal(err)
	// } else {
	// 	fmt.Printf("Error file opened\n")
	// }
	// defer errorFile.Close()

	// Create error file
	newErrorFile, err := os.Create("new_errors.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer newErrorFile.Close()
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
	// zipCodes := make(map[string]bool)
	largeLines := 0

	// Read the zip codes from errors.csv
	// errorScanner := bufio.NewScanner(errorFile)
	// for errorScanner.Scan() {
	// 	fields := strings.Split(errorScanner.Text(), ",")
	// 	if len(fields) >= 2 {
	// 		// fmt.Printf("Adding %s to zipCodes\n", fields[1])
	// 		zipCodes[fields[1]] = true
	// 	}
	// }

	// Iterate over each line in the file
	for scanner.Scan() {
		// Parse the line into a struct
		var input InputType
		err := json.Unmarshal(scanner.Bytes(), &input)
		if err != nil {
			fmt.Println("Error unmarshaling")
			log.Fatal(err)
		}

		// Convert the InputType to an OutputType
		obj := OutputType{
			Zip:     input.Zip,
			Center:  [2]float64{input.CenterLat, input.CenterLng},
			Outline: make([][2]float64, len(input.Outline)),
		}
		for i, latLng := range input.Outline {
			obj.Outline[i] = [2]float64{latLng.Lat, latLng.Lng}
		}

		// Calculate the size of obj in KB
		// objBytes, err := json.Marshal(obj)
		// if err != nil {
		// 	fmt.Println("Error marshaling")
		// 	log.Fatal(err)
		// }
		// objSize := float64(len(objBytes)) / 1024.0

		if counter < cursor {
			fmt.Printf("Skipping %d\n", counter)
		} else {

			// Check if the zip code is in the errors.csv file
			// if zipCodes[strconv.Itoa(obj.Zip)] {
			fmt.Printf("Processing %d\n", counter)

			// Check if the size of obj is over 400KB
			// if objSize > 400.0 {
			// 	fmt.Printf("objSize: %f for zip %d\n", objSize, obj.Zip)
			// 	// fmt.Printf("obj: %v\n", obj)
			// 	largeLines++
			// }

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
				// Convert the OutputType to a DynamoDB item
				// Convert the OutputType to a DynamoDB item
				item := map[string]*dynamodb.AttributeValue{
					"Zip": {
						N: aws.String(strconv.Itoa(obj.Zip)),
					},
					"Center": {
						L: []*dynamodb.AttributeValue{
							{N: aws.String(strconv.FormatFloat(obj.Center[0], 'f', -1, 64))},
							{N: aws.String(strconv.FormatFloat(obj.Center[1], 'f', -1, 64))},
						},
					},
					"Outline": {
						L: make([]*dynamodb.AttributeValue, len(obj.Outline)),
					},
				}
				for i, tuple := range obj.Outline {
					item["Outline"].L[i] = &dynamodb.AttributeValue{
						M: map[string]*dynamodb.AttributeValue{
							"Lat": {N: aws.String(strconv.FormatFloat(tuple[0], 'f', -1, 64))},
							"Lng": {N: aws.String(strconv.FormatFloat(tuple[1], 'f', -1, 64))},
						},
					}
				}
				putItemInput := &dynamodb.PutItemInput{
					TableName: aws.String("geo_zip"),
					Item:      item,
				}
				_, err = svc.PutItem(putItemInput)
				if err != nil {
					fmt.Println("Error inserting item:", err)
					newErrorFile.WriteString(fmt.Sprintf("%d,%d,%s\n", counter, obj.Zip, strings.ReplaceAll(strings.ReplaceAll(err.Error(), "\n", ""), ",", "")))
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
								{N: aws.String(strconv.FormatFloat(obj.Center[0], 'f', -1, 64))},
								{N: aws.String(strconv.FormatFloat(obj.Center[1], 'f', -1, 64))},
							},
						},
						":outline": {
							L: make([]*dynamodb.AttributeValue, len(obj.Outline)),
						},
					},
				}
				_, err = svc.UpdateItem(updateItemInput)
				if err != nil {
					fmt.Println("Error updating item:", err)
					newErrorFile.WriteString(fmt.Sprintf("%d,%d,%s\n", counter, obj.Zip, strings.ReplaceAll(strings.ReplaceAll(err.Error(), "\n", ""), ",", "")))
				}
			}
			// } else {
			// // If the zip code is not in the errors.csv file, insert a new item
			// // Skip for now
			// putItemInput := &dynamodb.PutItemInput{
			// 	TableName: aws.String("geo_zip"),
			// 	Item: map[string]*dynamodb.AttributeValue{
			// 		"Zip": {
			// 			N: aws.String(strconv.Itoa(obj.Zip)),
			// 		},
			// 		"Center": {
			// 			L: []*dynamodb.AttributeValue{
			// 				{N: aws.String(strconv.FormatFloat(obj.CenterLat, 'f', -1, 64))},
			// 				{N: aws.String(strconv.FormatFloat(obj.CenterLng, 'f', -1, 64))},
			// 			},
			// 		},
			// 		"Outline": {
			// 			S: aws.String(outlineStr),
			// 		},
			// 	},
			// }
			// _, err = svc.PutItem(putItemInput)
			// if err != nil {
			// 	fmt.Println("Error inserting item:", err)
			// 	newErrorFile.WriteString(fmt.Sprintf("%d,%d,%s\n", counter, obj.Zip, strings.ReplaceAll(strings.ReplaceAll(err.Error(), "\n", ""), ",", "")))
			// }
			// fmt.Printf("Skipping item #%d, zip %d\n", counter, obj.Zip)
			// }
		}

		counter++
	}

	// fmt.Printf("Output written to DynamoDB\n")
	// Print the number of lines over 400KB
	fmt.Printf("Number of lines over 400KB: %d\n", largeLines)
}
