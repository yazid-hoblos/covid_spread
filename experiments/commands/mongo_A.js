// Workload A: total cases per country (MongoDB)
const db = (typeof db !== 'undefined' && db) ? db.getSiblingDB('covid') : connect('mongodb://127.0.0.1:27017').getDB('covid');

const pipeline = [
  { $group: { _id: "$countriesAndTerritories", totalCases: { $sum: "$cases" } } },
  { $project: { _id: 0, country: "$_id", totalCases: 1 } },
  { $sort: { totalCases: -1 } }
];

// Execute aggregation and print the number of groups (and optionally a small sample)
const results = db.records.aggregate(pipeline).toArray();
printjson({ groups: results.length, sample: results.slice(0,5) });
