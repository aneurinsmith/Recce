
const setLocation = async (latlng) => {
    if(!latlng || latlng.length != 2) {
        await new Promise((resolve)=> {
            navigator.geolocation.getCurrentPosition((position) => {
                latlng = [position.coords.latitude, position.coords.longitude];
                resolve();
            }, async ()=> {
                var ipGL = await fetch('/recce/api/geolocate')
                .then(location => location.json());
                latlng = ipGL.coords;
                resolve();
            });
        });
    }

    window.history.replaceState(null, null, `/recce@${latlng[0]},${latlng[1]}${window.location.search}`);
}

const getLocation = () => {
    const latlng = window.location.pathname.match(/@(-?\d{1,2}\.\d+,\s*-?\d{1,3}\.\d+)/);
    if (latlng && latlng.length > 0) {
        return latlng[1].split(',')
    } else {
        return null
    }
}

const createElement = (HTML) => {
    var div = document.createElement('div');
    div.innerHTML = HTML.trim();

    return div.firstChild;
}
