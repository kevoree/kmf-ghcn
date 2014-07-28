class kmf.ghcn.DataSet {
    @contained
    stations : kmf.ghcn.Station[0,*]
    @contained
    countries : kmf.ghcn.Country[0,*]
    @contained
    usStates : kmf.ghcn.USState[0,*]
}

class  kmf.ghcn.Station {
    @id
    id : String
    country : kmf.ghcn.Country[0,1]
    latitude     : Float
    longitude : Float
    elevation : Float
    state : kmf.ghcn.USState[0,1]
    name : String
    gsnFlag : Bool
    hcnFlag : Bool
    wmoId : String
    @contained
    lastRecords : kmf.ghcn.Record[0,*]
}

class  kmf.ghcn.Country {
    @id
    id : String
    name : String
}

class  kmf.ghcn.USState {
    @id
    id : String
    name : String
}

class  kmf.ghcn.Record {
    @id
    type : String
    value : String
    measurement : String
    quality : String
    source : String
}