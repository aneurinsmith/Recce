
const gridDOM = document.getElementById("grid")

const getResults = async () => {
    return new Promise(async(resolve, reject) => {
        var xhr = new XMLHttpRequest();
        xhr.onreadystatechange = function() {
            if(this.readyState == XMLHttpRequest.DONE) {
                const results = JSON.parse(this.responseText)

                resolve(results)
    
                // var categories= {};
                // for (const result of results) {
                //     if (!categories[result.v.category[0]]) {
                //         categories[result.v.category[0]] = [result];
                //     } else {
                //         categories[result.v.category[0]].push(result);
                //     }
                // }
            }
        }
        var latlng = getLocation();
        xhr.open("GET", `/recce/api/${latlng[0]},${latlng[1]}`, true);
        xhr.send();
    });
}

const loadResults = async () => {
    await setLocation()
    for (const result of await getResults()) {
        gridDOM.appendChild(createElement(`
            <div class="card ${result.v.wikidata ? 'key' : ''} result">
                ${result.v.wikidata ? `
                    
                    
                    
                ` : ''}
                <div class="card-body">
                    <p class="card-text">${result.v.venue_name}</p>
                    <div>

                    </div>
                </div>
            </div>
        `))
        console.log(result)
    }
}

loadResults()