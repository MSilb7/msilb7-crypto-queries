// $ npm install json2csv
import * as fs from "fs";
import { addresses } from './mainnet';
import * as json2csv from "json2csv";

// const tsFile = "./mainnet.ts";

// // chatgpt generated... lol
// function flattenArray(arr: any[]): any[] {
//   // Create a new array to store the flattened result
//   const flattened : unknown[] = [];

//   // Iterate over the input array
//   for (const element of arr) {
//     // If the current element is an array, flatten it and concatenate it to the result
//     if (Array.isArray(element)) {
//       flattened.push(...flattenArray(element));
//     } else {
//       // Otherwise, just add the element to the result
//       flattened.push(element);
//     }
//   }

//   // Return the flattened array
//   return flattened;
// }


// Convert the TypeScript data to a regular JavaScript object
// const obj = eval();

let bonders_json = addresses.bonders
let rewards_json = addresses.rewardsContracts
let bridges_json = addresses.bridges

let json_array = [bonders_json,rewards_json,bridges_json]

// const csv = json2csv.parse(bridges_json);

// Flatten the JSON array
// const flattenedArray = flattenArray(bridges_json);

fs.writeFileSync("intermediates/hop_bonders.txt", JSON.stringify(bonders_json))
fs.writeFileSync("intermediates/hop_rewards.txt", JSON.stringify(rewards_json))
fs.writeFileSync("intermediates/hop_bridges.txt", JSON.stringify(bridges_json))


// // Output the CSV string
// console.log(csv);