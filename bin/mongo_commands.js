// ---------- Simple aggregations ----------
// 1) Total cases per country
db.records.aggregate([
  { $group: { _id: "$countriesAndTerritories", totalCases: { $sum: "$cases" } } },
  { $project: { _id: 0, country: "$_id", totalCases: 1 } },
  { $sort: { totalCases: -1 } }
])

// 2) Total deaths per continent
db.records.aggregate([
  { $group: { _id: "$continentExp", totalDeaths: { $sum: "$deaths" } } },
  { $project: { _id: 0, continent: "$_id", totalDeaths: 1 } },
  { $sort: { totalDeaths: -1 } }
])

// ---------- Export aggregation results for external visualization ----------
// Use mongoexport (terminal command) to dump a collection produced by $out
// mongoexport --db covid --collection cases_per_country --out mongo_results.json

// ---------- Cumulative totals using $setWindowFields (MongoDB 5.0+) ----------
// Compute cumulative cases and deaths by continent in date order and write to a collection
db.records.aggregate([
  { $sort: { continentExp: 1, dateRep: 1 } },
  { $setWindowFields: {
      partitionBy: "$continentExp",
      sortBy: { dateRep: 1 },
      output: {
        cumulativeCases: { $sum: "$cases", window: { documents: ["unbounded", "current"] } },
        cumulativeDeaths: { $sum: "$deaths", window: { documents: ["unbounded", "current"] } }
      }
  }},
  { $project: { _id: 0, continent: "$continentExp", date: "$dateRep", cumulativeCases: 1, cumulativeDeaths: 1 } },
  { $out: "results_cumulative_continent" }
])

// ---------- Per-country daily cumulative time series ----------
// If you need a per-country cumulative time series, partition by country instead:
db.records.aggregate([
  { $sort: { countriesAndTerritories: 1, dateRep: 1 } },
  { $setWindowFields: {
      partitionBy: "$countriesAndTerritories",
      sortBy: { dateRep: 1 },
      output: {
        cumulativeCases: { $sum: "$cases", window: { documents: ["unbounded", "current"] } },
        cumulativeDeaths: { $sum: "$deaths", window: { documents: ["unbounded", "current"] } }
      }
  }},
  { $project: { _id: 0, country: "$countriesAndTerritories", date: "$dateRep", dailyCases: "$cases", cumulativeCases: 1, cumulativeDeaths: 1 } },
  { $out: "results_daily_cumulative_country" }
])

// ---------- Cases / deaths per 100k inhabitants ----------
// This aggregation groups totals by country, carries population (popData2019) and computes rates
db.records.aggregate([
  { $group: {
      _id: "$countriesAndTerritories",
      totalCases: { $sum: "$cases" },
      totalDeaths: { $sum: "$deaths" },
      population: { $first: "$popData2019" }
  }},
  { $project: {
      _id: 0,
      country: "$_id",
      totalCases: 1,
      totalDeaths: 1,
      population: 1,
      casesPer100k: { $multiply: [{ $divide: ["$totalCases", "$population"] }, 100000] },
      deathsPer100k: { $multiply: [{ $divide: ["$totalDeaths", "$population"] }, 100000] }
  }},
  { $sort: { casesPer100k: -1 } },
  { $out: "results_per_100k" }
])

// ---------- 14-day incidence: convert string to numeric and compute stats ----------
// The raw field name in the dataset is: Cumulative_number_for_14_days_of_COVID-19_cases_per_100000
db.records.aggregate([
  { $addFields: {
      incidenceNum: {
        $convert: {
          input: "$Cumulative_number_for_14_days_of_COVID-19_cases_per_100000",
          to: "double",
          onError: null,
          onNull: null
        }
      }
  }},
  { $match: { incidenceNum: { $ne: null } } },
  { $group: {
      _id: "$countriesAndTerritories",
      avg14DayIncidence: { $avg: "$incidenceNum" },
      max14DayIncidence: { $max: "$incidenceNum" },
      min14DayIncidence: { $min: "$incidenceNum" }
  }},
  { $sort: { avg14DayIncidence: -1 } },
  { $out: "results_14day_incidence" }
])

// ---------- Weekly and Monthly Aggregations ----------
// Weekly aggregates (ISO week)
db.records.aggregate([
  { $addFields: {
      dateObj: { $dateFromString: { dateString: "$dateRep", format: "%d/%m/%Y" } },
      isoWeek: { $isoWeek: { $dateFromString: { dateString: "$dateRep", format: "%d/%m/%Y" } } },
      year: { $year: { $dateFromString: { dateString: "$dateRep", format: "%d/%m/%Y" } } }
  }},
  { $group: {
      _id: { country: "$countriesAndTerritories", year: "$year", week: "$isoWeek" },
      weeklyCases: { $sum: "$cases" },
      weeklyDeaths: { $sum: "$deaths" }
  }},
  { $sort: { "_id.country": 1, "_id.year": 1, "_id.week": 1 } },
  { $project: {
      _id: 0,
      country: "$_id.country",
      year: "$_id.year",
      week: "$_id.week",
      weeklyCases: 1,
      weeklyDeaths: 1
  }},
  { $out: "results_weekly_trends" }
])

// Monthly aggregates
db.records.aggregate([
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
  { $sort: { "_id.country": 1, "_id.year": 1, "_id.month": 1 } },
  { $project: {
      _id: 0,
      country: "$_id.country",
      year: "$_id.year",
      month: "$_id.month",
      monthlyCases: 1,
      monthlyDeaths: 1
  }},
  { $out: "results_monthly_trends" }
])

// ---------- Example: case fatality ratio per country (deaths / cases) ----------
db.records.aggregate([
  { $group: { _id: "$countriesAndTerritories", totalCases: { $sum: "$cases" }, totalDeaths: { $sum: "$deaths" } } },
  { $addFields: {
      caseFatalityRatio: {
        $cond: { if: { $gt: ["$totalCases", 0] }, then: { $divide: ["$totalDeaths", "$totalCases"] }, else: 0 }
      }
  }},
  { $project: { _id: 0, country: "$_id", totalCases: 1, totalDeaths: 1, caseFatalityRatio: 1 } },
  { $out: "results_case_fatality_country" }
])

// ---------- Export a results collection to JSON (terminal, not mongosh) ----------
// mongoexport --db covid --collection results_case_fatality_country --out results_case_fatality_country.json
