// Workload C: monthly aggregates (MongoDB)
// Make the script compatible with both mongosh and the legacy mongo shell.
const db = (typeof db !== 'undefined' && db) ? db.getSiblingDB('covid') : connect('mongodb://127.0.0.1:27017').getDB('covid');

const pipeline = [
  { $addFields: {
      dateObj: { $dateFromString: { dateString: "$dateRep", format: "%d/%m/%Y" } },
      month: { $month: { $dateFromString: { dateString: "$dateRep", format: "%d/%m/%Y" } } },
      year: { $year: { $dateFromString: { dateString: "$dateRep", format: "%d/%m/%Y" } } }
  }},
  { $group: {
      _id: { country: "$countriesAndTerritories", year: "$year", month: "$month" },
      monthlyCases: { $sum: "$cases" },
      monthlyDeaths: { $sum: "$deaths" }
  }},
  { $project: {
      _id: 0,
      country: "$_id.country",
      year: "$_id.year",
      month: "$_id.month",
      monthlyCases: 1,
      monthlyDeaths: 1
  }},
  { $out: "results_monthly_trends" }
];

// Execute aggregation and print a small summary
const results = db.records.aggregate(pipeline).toArray();
printjson({ groups: results.length, sample: results.slice(0,5) });
