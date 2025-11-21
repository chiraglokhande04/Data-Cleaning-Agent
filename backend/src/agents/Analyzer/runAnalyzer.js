const dfd = require("danfojs-node")
const { Analyzer } = require("./Analyzer.js")

(async() => {

    const df =  await dfd.readCSV("data.csv")
    const analyzer = new Analyzer(df)
    const result = analyzer.runAll()

    console.log(JSON.stringify(result,null,2))
})() ;