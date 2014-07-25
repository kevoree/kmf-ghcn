class kmf.ghcnd.DataSet {
    @contained
    stations : kmf.ghcnd.Station[0,*]
    @contained
    countries : kmf.ghcnd.Country[0,*]
    @contained
    usStates : kmf.ghcnd.USState[0,*]
}

class  kmf.ghcnd.Station {
    @id
    id : String
    country : kmf.ghcnd.Country[0,1]
    latitude     : Float
    longitude : Float
    elevation : Float
    state : kmf.ghcnd.USState[0,1]
    name : String
    gsnFlag : Bool
    hcnFlag : Bool
    wmoId : String
    @contained
    lastRecords : kmf.ghcnd.Record[0,*]
}

class  kmf.ghcnd.Country {
    @id
    id : String
    name : String
}

class  kmf.ghcnd.USState {
    @id
    id : String
    name : String
}

class  kmf.ghcnd.Record {
    @id
    type : String
    value : String
    measurement : String
    quality : String
    source : String
}