// Workload B: case fatality ratio per country (MongoDB)
// Use a mongosh-friendly DB handle instead of the shell-only `use` command.
const db = (typeof db !== 'undefined' && db) ? db.getSiblingDB('covid') : connect('mongodb://127.0.0.1:27017').getDB('covid');

const pipeline = [
  { $group: { _id: "$countriesAndTerritories", totalCases: { $sum: "$cases" }, totalDeaths: { $sum: "$deaths" } } },
  { $addFields: {
      caseFatalityRatio: {
        $cond: { if: { $gt: ["$totalCases", 0] }, then: { $divide: ["$totalDeaths", "$totalCases"] }, else: 0 }
      }
  }},
  { $project: { _id: 0, country: "$_id", totalCases: 1, totalDeaths: 1, caseFatalityRatio: 1 } },
  { $sort: { caseFatalityRatio: -1 } }
];

// Execute aggregation and print a small JSON summary (count and top 5)
const results = db.records.aggregate(pipeline).toArray();
printjson({ groups: results.length, sample: results.slice(0,5) });
