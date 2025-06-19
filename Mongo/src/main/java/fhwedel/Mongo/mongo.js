// a
db.buecher.insertOne({
  "INVNR": "123456",
  "Autor": "Marc-Uwe Kling",
  "Titel": "Die Känguru-Chroniken: Ansichten eines vorlauten Beuteltiers",
  "Verlag": "Ullstein-Verlag"
});

db.leser.insertOne({
  "LNR": "1001",
  "Name": "Friedrich Funke",
  "Adresse": "Bahnhofstraße 17, 23758 Oldenburg"
});

db.buecher.insertMany([
  {
    "INVNR": "123457",
    "Autor": "George Orwell",
    "Titel": "1984",
    "Verlag": "Ullstein"
  },
  {
    "INVNR": "123458",
    "Autor": "J.K. Rowling",
    "Titel": "Harry Potter und der Stein der Weisen",
    "Verlag": "Carlsen"
  },
  {
    "INVNR": "123459",
    "Autor": "Frank Schätzing",
    "Titel": "Der Schwarm",
    "Verlag": "Fischer"
  },
  {
    "INVNR": "123460",
    "Autor": "Hape Kerkeling",
    "Titel": "Ich bin dann mal weg",
    "Verlag": "Piper"
  },
  {
    "INVNR": "123461",
    "Autor": "Stephen King",
    "Titel": "Es",
    "Verlag": "Heyne"
  }
]);

db.leser.insertMany([
  {
    "LNR": "1002",
    "Name": "Anna Müller",
    "Adresse": "Lindenweg 3, 10115 Berlin"
  },
  {
    "LNR": "1003",
    "Name": "Ben Schröder",
    "Adresse": "Am Hang 12, 80331 München"
  },
  {
    "LNR": "1004",
    "Name": "Clara Becker",
    "Adresse": "Marktplatz 5, 50667 Köln"
  },
  {
    "LNR": "1005",
    "Name": "Daniel Schmidt",
    "Adresse": "Hauptstraße 9, 20095 Hamburg"
  },
  {
    "LNR": "1006",
    "Name": "Eva Lange",
    "Adresse": "Bergstraße 14, 04109 Leipzig"
  }
]);

db.entleihen.insertOne({
  "LNR": "1001",
  "INVNR": "123457",
  "Rueckgabedatum": "2025-07-15"
});

db.entleihen.insertOne({
  "LNR": "1001",
  "INVNR": "123458",
  "Rueckgabedatum": "2025-07-15"
});

// b
db.buecher.find(
  { "Autor": "Marc-Uwe Kling" },
)

// c
db.buecher.countDocuments()

// d
db.entleihen.aggregate([
  {
    $group: {
      _id: "$LNR",
      anzahlBuecher: { $sum: 1 }
    }
  },
  {
    $match: {
      anzahlBuecher: { $gt: 1 }
    }
  },
  {
    $sort: {
      anzahlBuecher: -1
    }
  },
  {
    $lookup: {
      from: "leser",
      localField: "_id",
      foreignField: "LNR",
      as: "leserInfo"
    }
  }
])

// e
db.entleihen.insertOne({
  "LNR": "1001",
  "INVNR": "123456",
  "Rueckgabedatum": "2025-07-10"
})

db.entleihen.deleteOne({
  "LNR": "1001",
  "INVNR": "123456"
})

// f
db.leser.insertOne({
  "LNR": "1007",
  "Name": "Heinz Müller",
  "Adresse": "Klopstockweg 17, 38124 Braunschweig",
  "entleihen": [
    {
      "Titel": "Der König von Berlin",
      "Autor": "Horst Evers",
      "Verlag": "Rowohlt-Verlag",
      "Rueckgabedatum": "2025-08-01"
    },
  ]
})

// g
db.leser.updateOne(
  { "LNR": "1007" },
  {
    $pull: {
      "entleihen": {
        "Titel": "Der König von Berlin",
      }
    }
  }
)

db.leser.updateOne(
  { "LNR": "1001" },
  {
    $push: {
      "entleihen": {
        "Titel": "Der König von Berlin",
        "Autor": "Horst Evers",
        "Verlag": "Rowohlt-Verlag",
        "Rueckgabedatum": "2025-08-01"
      }
    }
  }
)
