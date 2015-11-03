class kmf.ghcn.DataSet {
    rel stations : kmf.ghcn.Station
    rel countries : kmf.ghcn.Country
    rel usStates : kmf.ghcn.USState
}

class kmf.ghcn.Station {
    att id : String
    att name : String
    att gsnFlag : Bool
    att hcnFlag : Bool
    att wmoId : String
    att latitude : Double
    att longitude : Double
    att elevation : Double
    rel state : kmf.ghcn.USState with maxBound 1
    rel country : kmf.ghcn.Country with maxBound 1
    rel records : kmf.ghcn.Record
}

class  kmf.ghcn.Country {
    att id : String
    att name : String
}

class  kmf.ghcn.USState {
    att id : String
    att name : String
}

class  kmf.ghcn.Record {
    att type : String
    att value : String
    att measurement : String
    att quality : String
    att source : String
}