db.actors.insert({actor_id:"a1", name:"actor name"});
db.actors.insert({actor_id:"a2", name:"another name"});
db.movies.insert({actor_id:"a1", title:"movie1"});
db.movies.insert({actor_id:"a2", title:"movie2"});

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
      title : "" 
    };

    values.forEach(function(value) {
        if(value.name != null) {
            result.name = value.name;
        }
         if(value.title != null) {
            result.title = value.title;
        }
    });
    return result;
   
}

res = db.actors.mapReduce(actors_map, r, {out: {reduce : "joined"}});
res = db.movies.mapReduce(movies_map, r, {out: {reduce : "joined"}});

res.find();
