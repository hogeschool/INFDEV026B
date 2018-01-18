db.actors.insert({actor_id:"a1", name:"actor name",age:35});
db.actors.insert({actor_id:"a2", name:"another name", age: 46});
db.movies.insert({actor_id:"a1", title:"movie1"});
db.movies.insert({actor_id:"a1", title:"movie2"});
db.movies.insert({actor_id:"a2", title:"movie3"});

//map for actors
actors_map = function() {
  emit(this.actor_id, {"name" : this.name});
};

//map for movies
movies_map = function() {
  emit(this.actor_id, {"title" : this.title});
};

//reduce for result sets
r = function(key, values) {
  var result = {
      name : "",
      titles : [] 
    };

    values.forEach(function(value) {
        if(value.name != null) {
            result.name = value.name;
        }
        if(value.title != null) {
            result.titles.push(value.title);
        }
        if(value.titles != null) {
            value.titles.forEach( function(title) {result.titles.push(title)});
        }

    });
    return result;
   
}

res = db.actors.mapReduce(actors_map, r, {out: {reduce : "joined"}});
res = db.movies.mapReduce(movies_map, r, {out: {reduce : "joined"}});

res.find();
