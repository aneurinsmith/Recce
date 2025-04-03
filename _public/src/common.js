
const setLocation = async (latlng) => {
    if(!latlng || latlng.length != 2) {
        await new Promise((resolve)=> {
            navigator.geolocation.getCurrentPosition((position) => {
                latlng = [position.coords.latitude, position.coords.longitude];
                resolve();
            }, async ()=> {
                var ipGC = await fetch('https://api.geoapify.com/v1/ipinfo?apiKey=aa97ef9528a247539cba59fcc394a411')
                .then(location => location.json());
                latlng = [ipGC.location.latitude, ipGC.location.longitude];
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
