const dfd = require("danfojs-node");
const { Cleaner } = require("./cleaner");
const {
    MapCategorical,
    FillMissing,
    DeriveColumn,
} = require("./transformations");

(async () => {
    const df = await dfd.readCSV("data.csv");
    const cleaner = new Cleaner(df);

    // 1. Fill missing values with "Unknown"
    const fillCompany = new FillMissing({
        column: "Company",
        strategy: "constant",
        value: "Unknown",
    });
    cleaner.applyTransformation(fillCompany, { autoClean: true });

    const fillPos = new FillMissing({
        column: "Position",
        strategy: "constant",
        value: "Unknown",
    });
    cleaner.applyTransformation(fillPos, { autoClean: true });

    const fuzzyCompany = new MapCategorical({
        column: "Company",
        fuzzy: true,
        threshold: 0.85,
    });
    cleaner.applyTransformation(fuzzyCompany, { autoClean: true });

    const fuzzyPos = new MapCategorical({
        column: "Position",
        fuzzy: true,
        threshold: 0.85,
    });
    cleaner.applyTransformation(fuzzyPos, { autoClean: true });

    const nameLength = new DeriveColumn({
        new_column: "name_len",
        fn: (row) => (row.Name ? row.Name.length : 0),
    });
    cleaner.applyTransformation(nameLength, { autoClean: true });


    // 5. Export
    console.log("Snapshot:", cleaner.snapshot());
    const finalDf = cleaner.getCurrentDf();

    await dfd.toCSV(finalDf, {
        filePath: __dirname + "/data/employees_cleaned.csv"
    });

    console.log("File saved!");

})();
