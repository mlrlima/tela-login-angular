var ND=Object.defineProperty,kD=Object.defineProperties;var OD=Object.getOwnPropertyDescriptors;var bv=Object.getOwnPropertySymbols;var FD=Object.prototype.hasOwnProperty,PD=Object.prototype.propertyIsEnumerable;var Cv=(t,n,e)=>n in t?ND(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,C=(t,n)=>{for(var e in n||={})FD.call(n,e)&&Cv(t,e,n[e]);if(bv)for(var e of bv(n))PD.call(n,e)&&Cv(t,e,n[e]);return t},H=(t,n)=>kD(t,OD(n));var De=(t,n,e)=>new Promise((i,r)=>{var o=c=>{try{a(e.next(c))}catch(l){r(l)}},s=c=>{try{a(e.throw(c))}catch(l){r(l)}},a=c=>c.done?i(c.value):Promise.resolve(c.value).then(o,s);a((e=e.apply(t,n)).next())});var zt=null,qc=!1,wr=1,LD=null,qe=Symbol("SIGNAL");function G(t){let n=zt;return zt=t,n}function Yc(){return zt}var Vi={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function ji(t){if(qc)throw new Error("");if(zt===null)return;zt.consumerOnSignalRead(t);let n=zt.producersTail;if(n!==void 0&&n.producer===t)return;let e,i=zt.recomputing;if(i&&(e=n!==void 0?n.nextProducer:zt.producers,e!==void 0&&e.producer===t)){zt.producersTail=e,e.lastReadVersion=t.version,e.knownValidAtEpoch=wr;return}let r=t.consumersTail;if(r!==void 0&&r.consumer===zt&&(!i||r.knownValidAtEpoch===wr))return;let o=Ao(zt),s={producer:t,consumer:zt,nextProducer:e,prevConsumer:void 0,knownValidAtEpoch:wr,lastReadVersion:t.version,nextConsumer:void 0};zt.producersTail=s,n!==void 0?n.nextProducer=s:zt.producers=s,o&&xv(t,s)}function Ev(){wr++}function Ir(t){if(!(Ao(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===wr)){if(!t.producerMustRecompute(t)&&!To(t)){Mo(t);return}t.producerRecomputeValue(t),Mo(t)}}function Af(t){if(t.consumers===void 0)return;let n=qc;qc=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||VD(i)}}finally{qc=n}}function Rf(){return zt?.consumerAllowSignalWrites!==!1}function VD(t){t.dirty=!0,Af(t),t.consumerMarkedDirty?.(t)}function Mo(t){t.dirty=!1,t.lastCleanEpoch=wr}function fi(t){return t&&wv(t),G(t)}function wv(t){if(t.producersTail?.knownValidAtEpoch===wr){let n=t.producers;for(;n!==void 0;)n.knownValidAtEpoch=null,n=n.nextProducer}t.producersTail=void 0,t.recomputing=!0}function Bi(t,n){G(n),t&&Dv(t)}function Dv(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(Ao(t))do e=Nf(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function To(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,i=n.lastReadVersion;if(i!==e.version||(Ir(e),i!==e.version))return!0}return!1}function Ui(t){if(Ao(t)){let n=t.producers;for(;n!==void 0;)n=Nf(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function xv(t,n){let e=t.consumersTail,i=Ao(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!i)for(let r=t.producers;r!==void 0;r=r.nextProducer)xv(r.producer,r)}function Nf(t){let n=t.producer,e=t.nextProducer,i=t.nextConsumer,r=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:n.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(n.consumers=i,!Ao(n)){let o=n.producers;for(;o!==void 0;)o=Nf(o)}return e}function Ao(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function Gs(t){LD?.(t)}function Ws(t,n){return Object.is(t,n)}function qs(t,n){let e=Object.create(jD);e.computation=t,n!==void 0&&(e.equal=n);let i=()=>{if(Ir(e),ji(e),e.value===Yn)throw e.error;return e.value};return i[qe]=e,Gs(e),i}var Dr=Symbol("UNSET"),xr=Symbol("COMPUTING"),Yn=Symbol("ERRORED"),jD=H(C({},Vi),{value:Dr,dirty:!0,error:null,equal:Ws,kind:"computed",producerMustRecompute(t){return t.value===Dr||t.value===xr},producerRecomputeValue(t){if(t.value===xr)throw new Error("");let n=t.value;t.value=xr;let e=fi(t),i,r=!1;try{i=t.computation(),G(null),r=n!==Dr&&n!==Yn&&i!==Yn&&t.equal(n,i)}catch(o){i=Yn,t.error=o}finally{Bi(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function BD(){throw new Error}var Iv=BD;function Sv(t){Iv(t)}function kf(t){Iv=t}var UD=null;function Of(t,n){let e=Object.create(Ys);e.value=t,n!==void 0&&(e.equal=n);let i=()=>Mv(e);return i[qe]=e,Gs(e),[i,s=>Sr(e,s),s=>Zc(e,s)]}function Mv(t){return ji(t),t.value}function Sr(t,n){Rf()||Sv(t),t.equal(t.value,n)||(t.value=n,HD(t))}function Zc(t,n){Rf()||Sv(t),Sr(t,n(t.value))}var Ys=H(C({},Vi),{equal:Ws,value:void 0,kind:"signal"});function HD(t){t.version++,Ev(),Af(t),UD?.(t)}var Ff=H(C({},Vi),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Pf(t){if(t.dirty=!1,t.version>0&&!To(t))return;t.version++;let n=fi(t);try{t.cleanup(),t.fn()}finally{Bi(t,n)}}var Lf;function Kc(){return Lf}function Zn(t){let n=Lf;return Lf=t,n}var Tv=Symbol("NotFound");function Ro(t){return t===Tv||t?.name==="\u0275NotFound"}function Vf(t,n,e){let i=Object.create(zD);i.source=t,i.computation=n,e!=null&&(i.equal=e);let o=()=>{if(Ir(i),ji(i),i.value===Yn)throw i.error;return i.value};return o[qe]=i,Gs(i),o}function Av(t,n){Ir(t),Sr(t,n),Mo(t)}function Rv(t,n){if(Ir(t),t.value===Yn)throw t.error;Zc(t,n),Mo(t)}var zD=H(C({},Vi),{value:Dr,dirty:!0,error:null,equal:Ws,kind:"linkedSignal",producerMustRecompute(t){return t.value===Dr||t.value===xr},producerRecomputeValue(t){if(t.value===xr)throw new Error("");let n=t.value;t.value=xr;let e=fi(t),i,r=!1;try{let o=t.source(),s=n!==Dr&&n!==Yn,a=s?{source:t.sourceValue,value:n}:void 0;i=t.computation(o,a),t.sourceValue=o,G(null),r=s&&i!==Yn&&t.equal(n,i)}catch(o){i=Yn,t.error=o}finally{Bi(t,e)}if(r){t.value=n;return}t.value=i,t.version++}});function Nv(t){let n=G(null);try{return t()}finally{G(n)}}function J(t){return typeof t=="function"}function No(t){let e=t(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var Qc=No(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function Mr(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var fe=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(J(i))try{i()}catch(o){n=o instanceof Qc?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{kv(o)}catch(s){n=n??[],s instanceof Qc?n=[...n,...s.errors]:n.push(s)}}if(n)throw new Qc(n)}}add(n){var e;if(n&&n!==this)if(this.closed)kv(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&Mr(e,n)}remove(n){let{_finalizers:e}=this;e&&Mr(e,n),n instanceof t&&n._removeParent(this)}};fe.EMPTY=(()=>{let t=new fe;return t.closed=!0,t})();var jf=fe.EMPTY;function Xc(t){return t instanceof fe||t&&"closed"in t&&J(t.remove)&&J(t.add)&&J(t.unsubscribe)}function kv(t){J(t)?t():t.unsubscribe()}var Pn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var ko={setTimeout(t,n,...e){let{delegate:i}=ko;return i?.setTimeout?i.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=ko;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function Jc(t){ko.setTimeout(()=>{let{onUnhandledError:n}=Pn;if(n)n(t);else throw t})}function Zs(){}var Ov=Bf("C",void 0,void 0);function Fv(t){return Bf("E",void 0,t)}function Pv(t){return Bf("N",t,void 0)}function Bf(t,n,e){return{kind:t,value:n,error:e}}var Tr=null;function Oo(t){if(Pn.useDeprecatedSynchronousErrorHandling){let n=!Tr;if(n&&(Tr={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:i}=Tr;if(Tr=null,e)throw i}}else t()}function Lv(t){Pn.useDeprecatedSynchronousErrorHandling&&Tr&&(Tr.errorThrown=!0,Tr.error=t)}var Ar=class extends fe{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,Xc(n)&&n.add(this)):this.destination=WD}static create(n,e,i){return new hi(n,e,i)}next(n){this.isStopped?Hf(Pv(n),this):this._next(n)}error(n){this.isStopped?Hf(Fv(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Hf(Ov,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},$D=Function.prototype.bind;function Uf(t,n){return $D.call(t,n)}var zf=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(i){el(i)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(i){el(i)}else el(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){el(e)}}},hi=class extends Ar{constructor(n,e,i){super();let r;if(J(n)||!n)r={next:n??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&Pn.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),r={next:n.next&&Uf(n.next,o),error:n.error&&Uf(n.error,o),complete:n.complete&&Uf(n.complete,o)}):r=n}this.destination=new zf(r)}};function el(t){Pn.useDeprecatedSynchronousErrorHandling?Lv(t):Jc(t)}function GD(t){throw t}function Hf(t,n){let{onStoppedNotification:e}=Pn;e&&ko.setTimeout(()=>e(t,n))}var WD={closed:!0,next:Zs,error:GD,complete:Zs};var Fo=typeof Symbol=="function"&&Symbol.observable||"@@observable";function nn(t){return t}function $f(...t){return Gf(t)}function Gf(t){return t.length===0?nn:t.length===1?t[0]:function(e){return t.reduce((i,r)=>r(i),e)}}var re=(()=>{class t{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new t;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=YD(e)?e:new hi(e,i,r);return Oo(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=Vv(i),new i((r,o)=>{let s=new hi({next:a=>{try{e(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[Fo](){return this}pipe(...e){return Gf(e)(this)}toPromise(e){return e=Vv(e),new e((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return t.create=n=>new t(n),t})();function Vv(t){var n;return(n=t??Pn.Promise)!==null&&n!==void 0?n:Promise}function qD(t){return t&&J(t.next)&&J(t.error)&&J(t.complete)}function YD(t){return t&&t instanceof Ar||qD(t)&&Xc(t)}function ZD(t){return J(t?.lift)}function te(t){return n=>{if(ZD(n))return n.lift(function(e){try{return t(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function oe(t,n,e,i,r){return new Wf(t,n,e,i,r)}var Wf=class extends Ar{constructor(n,e,i,r,o,s){super(n),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(c){n.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){n.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){n.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var jv=No(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var S=(()=>{class t extends re{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new tl(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new jv}next(e){Oo(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){Oo(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){Oo(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?jf:(this.currentObservers=null,o.push(e),new fe(()=>{this.currentObservers=null,Mr(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new re;return e.source=this,e}}return t.create=(n,e)=>new tl(n,e),t})(),tl=class extends S{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,n)}error(n){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&i!==void 0?i:jf}};var st=class extends S{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:i}=this;if(n)throw e;return this._throwIfClosed(),i}next(n){super.next(this._value=n)}};var Ks={now(){return(Ks.delegate||Date).now()},delegate:void 0};var nl=class extends S{constructor(n=1/0,e=1/0,i=Ks){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;e||(i.push(n),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!n.closed;s+=i?1:2)n.next(o[s]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*n;if(n<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=e.now(),a=0;for(let c=1;c<i.length&&i[c]<=s;c+=2)a=c;a&&i.splice(0,a+1)}}};var il=class extends fe{constructor(n,e){super()}schedule(n,e=0){return this}};var Qs={setInterval(t,n,...e){let{delegate:i}=Qs;return i?.setInterval?i.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=Qs;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var rl=class extends il{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var i;if(this.closed)return this;this.state=n;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,i=0){return Qs.setInterval(n.flush.bind(n,this),i)}recycleAsyncId(n,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&Qs.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(n,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let i=!1,r;try{this.work(n)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,Mr(i,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var Po=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,i){return new this.schedulerActionCtor(this,n).schedule(i,e)}};Po.now=Ks.now;var ol=class extends Po{constructor(n,e=Po.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let i;this._active=!0;do if(i=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,i){for(;n=e.shift();)n.unsubscribe();throw i}}};var Xs=new ol(rl),Bv=Xs;var Ye=new re(t=>t.complete());function sl(t){return t&&J(t.schedule)}function qf(t){return t[t.length-1]}function al(t){return J(qf(t))?t.pop():void 0}function Kn(t){return sl(qf(t))?t.pop():void 0}function Uv(t,n){return typeof qf(t)=="number"?t.pop():n}function zv(t,n,e,i){function r(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(d){try{l(i.next(d))}catch(f){s(f)}}function c(d){try{l(i.throw(d))}catch(f){s(f)}}function l(d){d.done?o(d.value):r(d.value).then(a,c)}l((i=i.apply(t,n||[])).next())})}function Hv(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],i=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&i>=t.length&&(t=void 0),{value:t&&t[i++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function Rr(t){return this instanceof Rr?(this.v=t,this):new Rr(t)}function $v(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(t,n||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(g){return function(b){return Promise.resolve(b).then(g,f)}}function a(g,b){i[g]&&(r[g]=function(I){return new Promise(function(N,ie){o.push([g,I,N,ie])>1||c(g,I)})},b&&(r[g]=b(r[g])))}function c(g,b){try{l(i[g](b))}catch(I){m(o[0][3],I)}}function l(g){g.value instanceof Rr?Promise.resolve(g.value.v).then(d,f):m(o[0][2],g)}function d(g){c("next",g)}function f(g){c("throw",g)}function m(g,b){g(b),o.shift(),o.length&&c(o[0][0],o[0][1])}}function Gv(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof Hv=="function"?Hv(t):t[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=t[o]&&function(s){return new Promise(function(a,c){s=t[o](s),r(a,c,s.done,s.value)})}}function r(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var cl=t=>t&&typeof t.length=="number"&&typeof t!="function";function ll(t){return J(t?.then)}function dl(t){return J(t[Fo])}function ul(t){return Symbol.asyncIterator&&J(t?.[Symbol.asyncIterator])}function fl(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function KD(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var hl=KD();function pl(t){return J(t?.[hl])}function ml(t){return $v(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:i,done:r}=yield Rr(e.read());if(r)return yield Rr(void 0);yield yield Rr(i)}}finally{e.releaseLock()}})}function gl(t){return J(t?.getReader)}function Te(t){if(t instanceof re)return t;if(t!=null){if(dl(t))return QD(t);if(cl(t))return XD(t);if(ll(t))return JD(t);if(ul(t))return Wv(t);if(pl(t))return ex(t);if(gl(t))return tx(t)}throw fl(t)}function QD(t){return new re(n=>{let e=t[Fo]();if(J(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function XD(t){return new re(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function JD(t){return new re(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,Jc)})}function ex(t){return new re(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function Wv(t){return new re(n=>{nx(t,n).catch(e=>n.error(e))})}function tx(t){return Wv(ml(t))}function nx(t,n){var e,i,r,o;return zv(this,void 0,void 0,function*(){try{for(e=Gv(t);i=yield e.next(),!i.done;){let s=i.value;if(n.next(s),n.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}n.complete()})}function Kt(t,n,e,i=0,r=!1){let o=n.schedule(function(){e(),r?t.add(this.schedule(null,i)):this.unsubscribe()},i);if(t.add(o),!r)return o}function vl(t,n=0){return te((e,i)=>{e.subscribe(oe(i,r=>Kt(i,t,()=>i.next(r),n),()=>Kt(i,t,()=>i.complete(),n),r=>Kt(i,t,()=>i.error(r),n)))})}function _l(t,n=0){return te((e,i)=>{i.add(t.schedule(()=>e.subscribe(i),n))})}function qv(t,n){return Te(t).pipe(_l(n),vl(n))}function Yv(t,n){return Te(t).pipe(_l(n),vl(n))}function Zv(t,n){return new re(e=>{let i=0;return n.schedule(function(){i===t.length?e.complete():(e.next(t[i++]),e.closed||this.schedule())})})}function Kv(t,n){return new re(e=>{let i;return Kt(e,n,()=>{i=t[hl](),Kt(e,n,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){e.error(s);return}o?e.complete():e.next(r)},0,!0)}),()=>J(i?.return)&&i.return()})}function yl(t,n){if(!t)throw new Error("Iterable cannot be null");return new re(e=>{Kt(e,n,()=>{let i=t[Symbol.asyncIterator]();Kt(e,n,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function Qv(t,n){return yl(ml(t),n)}function Xv(t,n){if(t!=null){if(dl(t))return qv(t,n);if(cl(t))return Zv(t,n);if(ll(t))return Yv(t,n);if(ul(t))return yl(t,n);if(pl(t))return Kv(t,n);if(gl(t))return Qv(t,n)}throw fl(t)}function Le(t,n){return n?Xv(t,n):Te(t)}function L(...t){let n=Kn(t);return Le(t,n)}function Js(t,n){let e=J(t)?t:()=>t,i=r=>r.error(e());return new re(n?r=>n.schedule(i,0,r):i)}function ea(t){return!!t&&(t instanceof re||J(t.lift)&&J(t.subscribe))}var Nr=No(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function Jv(t){return t instanceof Date&&!isNaN(t)}function ne(t,n){return te((e,i)=>{let r=0;e.subscribe(oe(i,o=>{i.next(t.call(n,o,r++))}))})}var{isArray:ix}=Array;function rx(t,n){return ix(n)?t(...n):t(n)}function bl(t){return ne(n=>rx(t,n))}var{isArray:ox}=Array,{getPrototypeOf:sx,prototype:ax,keys:cx}=Object;function Cl(t){if(t.length===1){let n=t[0];if(ox(n))return{args:n,keys:null};if(lx(n)){let e=cx(n);return{args:e.map(i=>n[i]),keys:e}}}return{args:t,keys:null}}function lx(t){return t&&typeof t=="object"&&sx(t)===ax}function El(t,n){return t.reduce((e,i,r)=>(e[i]=n[r],e),{})}function Yf(...t){let n=Kn(t),e=al(t),{args:i,keys:r}=Cl(t);if(i.length===0)return Le([],n);let o=new re(dx(i,n,r?s=>El(r,s):nn));return e?o.pipe(bl(e)):o}function dx(t,n,e=nn){return i=>{e_(n,()=>{let{length:r}=t,o=new Array(r),s=r,a=r;for(let c=0;c<r;c++)e_(n,()=>{let l=Le(t[c],n),d=!1;l.subscribe(oe(i,f=>{o[c]=f,d||(d=!0,a--),a||i.next(e(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function e_(t,n,e){t?Kt(e,t,n):n()}function t_(t,n,e,i,r,o,s,a){let c=[],l=0,d=0,f=!1,m=()=>{f&&!c.length&&!l&&n.complete()},g=I=>l<i?b(I):c.push(I),b=I=>{o&&n.next(I),l++;let N=!1;Te(e(I,d++)).subscribe(oe(n,ie=>{r?.(ie),o?g(ie):n.next(ie)},()=>{N=!0},void 0,()=>{if(N)try{for(l--;c.length&&l<i;){let ie=c.shift();s?Kt(n,s,()=>b(ie)):b(ie)}m()}catch(ie){n.error(ie)}}))};return t.subscribe(oe(n,g,()=>{f=!0,m()})),()=>{a?.()}}function $t(t,n,e=1/0){return J(n)?$t((i,r)=>ne((o,s)=>n(i,o,r,s))(Te(t(i,r))),e):(typeof n=="number"&&(e=n),te((i,r)=>t_(i,r,t,e)))}function wl(t=1/0){return $t(nn,t)}function n_(){return wl(1)}function Lo(...t){return n_()(Le(t,Kn(t)))}function kr(t){return new re(n=>{Te(t()).subscribe(n)})}function ta(...t){let n=al(t),{args:e,keys:i}=Cl(t),r=new re(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),c=s,l=s;for(let d=0;d<s;d++){let f=!1;Te(e[d]).subscribe(oe(o,m=>{f||(f=!0,l--),a[d]=m},()=>c--,void 0,()=>{(!c||!f)&&(l||o.next(i?El(i,a):a),o.complete())}))}});return n?r.pipe(bl(n)):r}function i_(t=0,n,e=Bv){let i=-1;return n!=null&&(sl(n)?e=n:i=n),new re(r=>{let o=Jv(t)?+t-e.now():t;o<0&&(o=0);let s=0;return e.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function Qt(...t){let n=Kn(t),e=Uv(t,1/0),i=t;return i.length?i.length===1?Te(i[0]):wl(e)(Le(i,n)):Ye}function Se(t,n){return te((e,i)=>{let r=0;e.subscribe(oe(i,o=>t.call(n,o,r++)&&i.next(o)))})}function r_(t){return te((n,e)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let l=r;r=null,e.next(l)}s&&e.complete()},c=()=>{o=null,s&&e.complete()};n.subscribe(oe(e,l=>{i=!0,r=l,o||Te(t(l)).subscribe(o=oe(e,a,c))},()=>{s=!0,(!i||!o||o.closed)&&e.complete()}))})}function Dl(t,n=Xs){return r_(()=>i_(t,n))}function Or(t){return te((n,e)=>{let i=null,r=!1,o;i=n.subscribe(oe(e,void 0,void 0,s=>{o=Te(t(s,Or(t)(n))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function Vo(t,n){return J(n)?$t(t,n,1):$t(t,1)}function Zf(t,n=Xs){return te((e,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let l=o;o=null,i.next(l)}};function c(){let l=s+t,d=n.now();if(d<l){r=this.schedule(void 0,l-d),i.add(r);return}a()}e.subscribe(oe(i,l=>{o=l,s=n.now(),r||(r=n.schedule(c,t),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function o_(t){return te((n,e)=>{let i=!1;n.subscribe(oe(e,r=>{i=!0,e.next(r)},()=>{i||e.next(t),e.complete()}))})}function Gt(t){return t<=0?()=>Ye:te((n,e)=>{let i=0;n.subscribe(oe(e,r=>{++i<=t&&(e.next(r),t<=i&&e.complete())}))})}function xl(t,n=nn){return t=t??ux,te((e,i)=>{let r,o=!0;e.subscribe(oe(i,s=>{let a=n(s);(o||!t(r,a))&&(o=!1,r=a,i.next(s))}))})}function ux(t,n){return t===n}function s_(t=fx){return te((n,e)=>{let i=!1;n.subscribe(oe(e,r=>{i=!0,e.next(r)},()=>i?e.complete():e.error(t())))})}function fx(){return new Nr}function Fr(t){return te((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function pi(t,n){let e=arguments.length>=2;return i=>i.pipe(t?Se((r,o)=>t(r,o,i)):nn,Gt(1),e?o_(n):s_(()=>new Nr))}function Il(t){return t<=0?()=>Ye:te((n,e)=>{let i=[];n.subscribe(oe(e,r=>{i.push(r),t<i.length&&i.shift()},()=>{for(let r of i)e.next(r);e.complete()},void 0,()=>{i=null}))})}function Sl(){return te((t,n)=>{let e,i=!1;t.subscribe(oe(n,r=>{let o=e;e=r,i&&n.next([o,r]),i=!0}))})}function na(t={}){let{connector:n=()=>new S,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=t;return o=>{let s,a,c,l=0,d=!1,f=!1,m=()=>{a?.unsubscribe(),a=void 0},g=()=>{m(),s=c=void 0,d=f=!1},b=()=>{let I=s;g(),I?.unsubscribe()};return te((I,N)=>{l++,!f&&!d&&m();let ie=c=c??n();N.add(()=>{l--,l===0&&!f&&!d&&(a=Kf(b,r))}),ie.subscribe(N),!s&&l>0&&(s=new hi({next:ot=>ie.next(ot),error:ot=>{f=!0,m(),a=Kf(g,e,ot),ie.error(ot)},complete:()=>{d=!0,m(),a=Kf(g,i),ie.complete()}}),Te(I).subscribe(s))})(o)}}function Kf(t,n,...e){if(n===!0){t();return}if(n===!1)return;let i=new hi({next:()=>{i.unsubscribe(),t()}});return Te(n(...e)).subscribe(i)}function Ml(t,n,e){let i,r=!1;return t&&typeof t=="object"?{bufferSize:i=1/0,windowTime:n=1/0,refCount:r=!1,scheduler:e}=t:i=t??1/0,na({connector:()=>new nl(i,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function Qf(t){return Se((n,e)=>t<=e)}function _t(...t){let n=Kn(t);return te((e,i)=>{(n?Lo(t,e,n):Lo(t,e)).subscribe(i)})}function We(t,n){return te((e,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();e.subscribe(oe(i,c=>{r?.unsubscribe();let l=0,d=o++;Te(t(c,d)).subscribe(r=oe(i,f=>i.next(n?n(c,f,d,l++):f),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function ke(t){return te((n,e)=>{Te(t).subscribe(oe(e,()=>e.complete(),Zs)),!e.closed&&n.subscribe(e)})}function Xf(t,n=!1){return te((e,i)=>{let r=0;e.subscribe(oe(i,o=>{let s=t(o,r++);(s||n)&&i.next(o),!s&&i.complete()}))})}function Ze(t,n,e){let i=J(t)||n||e?{next:t,error:n,complete:e}:t;return i?te((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(oe(o,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),o.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),o.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),o.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):nn}var Fl="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",D=class extends Error{code;constructor(n,e){super(vi(n,e)),this.code=n}};function hx(t){return`NG0${Math.abs(t)}`}function vi(t,n){return`${hx(t)}${n?": "+n:""}`}function Ee(t){for(let n in t)if(t[n]===Ee)return n;throw Error("")}function u_(t,n){for(let e in n)n.hasOwnProperty(e)&&!t.hasOwnProperty(e)&&(t[e]=n[e])}function ca(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(ca).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function Pl(t,n){return t?n?`${t} ${n}`:t:n||""}var px=Ee({__forward_ref__:Ee});function hn(t){return t.__forward_ref__=hn,t}function yt(t){return uh(t)?t():t}function uh(t){return typeof t=="function"&&t.hasOwnProperty(px)&&t.__forward_ref__===hn}function V(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function z(t){return{providers:t.providers||[],imports:t.imports||[]}}function la(t){return mx(t,Ll)}function fh(t){return la(t)!==null}function mx(t,n){return t.hasOwnProperty(n)&&t[n]||null}function gx(t){let n=t?.[Ll]??null;return n||null}function eh(t){return t&&t.hasOwnProperty(Al)?t[Al]:null}var Ll=Ee({\u0275prov:Ee}),Al=Ee({\u0275inj:Ee}),y=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=V({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function hh(t){return t&&!!t.\u0275providers}var ph=Ee({\u0275cmp:Ee}),mh=Ee({\u0275dir:Ee}),gh=Ee({\u0275pipe:Ee}),vh=Ee({\u0275mod:Ee}),ra=Ee({\u0275fac:Ee}),Ur=Ee({__NG_ELEMENT_ID__:Ee}),a_=Ee({__NG_ENV_ID__:Ee});function f_(t){return Vl(t,"@NgModule"),t[vh]||null}function zi(t){return Vl(t,"@Component"),t[ph]||null}function _h(t){return Vl(t,"@Directive"),t[mh]||null}function h_(t){return Vl(t,"@Pipe"),t[gh]||null}function Vl(t,n){if(t==null)throw new D(-919,!1)}function jl(t){return typeof t=="string"?t:t==null?"":String(t)}var p_=Ee({ngErrorCode:Ee}),vx=Ee({ngErrorMessage:Ee}),_x=Ee({ngTokenPath:Ee});function yh(t,n){return m_("",-200,n)}function Bl(t,n){throw new D(-201,!1)}function m_(t,n,e){let i=new D(n,t);return i[p_]=n,i[vx]=t,e&&(i[_x]=e),i}function yx(t){return t[p_]}var th;function g_(){return th}function rn(t){let n=th;return th=t,n}function bh(t,n,e){let i=la(t);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(n!==void 0)return n;Bl(t,"")}var $i=globalThis;var bx={},Pr=bx,Cx="__NG_DI_FLAG__",nh=class{injector;constructor(n){this.injector=n}retrieve(n,e){let i=Lr(e)||0;try{return this.injector.get(n,i&8?null:Pr,i)}catch(r){if(Ro(r))return r;throw r}}};function Ex(t,n=0){let e=Kc();if(e===void 0)throw new D(-203,!1);if(e===null)return bh(t,void 0,n);{let i=wx(n),r=e.retrieve(t,i);if(Ro(r)){if(i.optional)return null;throw r}return r}}function M(t,n=0){return(g_()||Ex)(yt(t),n)}function u(t,n){return M(t,Lr(n))}function Lr(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function wx(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function ih(t){let n=[];for(let e=0;e<t.length;e++){let i=yt(t[e]);if(Array.isArray(i)){if(i.length===0)throw new D(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],c=Dx(a);typeof c=="number"?c===-1?r=a.token:o|=c:r=a}n.push(M(r,o))}else n.push(M(i))}return n}function Dx(t){return t[Cx]}function Vr(t,n){let e=t.hasOwnProperty(ra);return e?t[ra]:null}function v_(t,n,e){if(t.length!==n.length)return!1;for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function __(t){return t.flat(Number.POSITIVE_INFINITY)}function Ul(t,n){t.forEach(e=>Array.isArray(e)?Ul(e,n):n(e))}function Ch(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function da(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function y_(t,n){let e=[];for(let i=0;i<t;i++)e.push(n);return e}function b_(t,n,e,i){let r=t.length;if(r==n)t.push(e,i);else if(r===1)t.push(i,t[0]),t[0]=e;else{for(r--,t.push(t[r-1],t[r]);r>n;){let o=r-2;t[r]=t[o],r--}t[n]=e,t[n+1]=i}}function Hl(t,n,e){let i=Uo(t,n);return i>=0?t[i|1]=e:(i=~i,b_(t,i,n,e)),i}function zl(t,n){let e=Uo(t,n);if(e>=0)return t[e|1]}function Uo(t,n){return xx(t,n,1)}function xx(t,n,e){let i=0,r=t.length>>e;for(;r!==i;){let o=i+(r-i>>1),s=t[o<<e];if(n===s)return o<<e;s>n?r=o:i=o+1}return~(r<<e)}var Gi={},Tt=[],Hr=new y(""),ua=new y("",-1),Eh=new y(""),Bo=class{get(n,e=Pr){if(e===Pr){let r=m_("",-201);throw r.name="\u0275NotFound",r}return e}};function _i(t){return{\u0275providers:t}}function C_(t){return _i([{provide:Hr,multi:!0,useValue:t}])}function E_(...t){return{\u0275providers:wh(!0,t),\u0275fromNgModule:!0}}function wh(t,...n){let e=[],i=new Set,r,o=s=>{e.push(s)};return Ul(n,s=>{let a=s;Rl(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&w_(r,o),e}function w_(t,n){for(let e=0;e<t.length;e++){let{ngModule:i,providers:r}=t[e];Dh(r,o=>{n(o,i)})}}function Rl(t,n,e,i){if(t=yt(t),!t)return!1;let r=null,o=eh(t),s=!o&&zi(t);if(!o&&!s){let c=t.ngModule;if(o=eh(c),o)r=c;else return!1}else{if(s&&!s.standalone)return!1;r=t}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)Rl(l,n,e,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let l;Ul(o.imports,d=>{Rl(d,n,e,i)&&(l||=[],l.push(d))}),l!==void 0&&w_(l,n)}if(!a){let l=Vr(r)||(()=>new r);n({provide:r,useFactory:l,deps:Tt},r),n({provide:Eh,useValue:r,multi:!0},r),n({provide:Hr,useValue:()=>M(r),multi:!0},r)}let c=o.providers;if(c!=null&&!a){let l=t;Dh(c,d=>{n(d,l)})}}else return!1;return r!==t&&t.providers!==void 0}function Dh(t,n){for(let e of t)hh(e)&&(e=e.\u0275providers),Array.isArray(e)?Dh(e,n):n(e)}var Ix=Ee({provide:String,useValue:Ee});function D_(t){return t!==null&&typeof t=="object"&&Ix in t}function Sx(t){return!!(t&&t.useExisting)}function Mx(t){return!!(t&&t.useFactory)}function jr(t){return typeof t=="function"}function x_(t){return!!t.useClass}var fa=new y(""),Tl={},c_={},Jf;function Ho(){return Jf===void 0&&(Jf=new Bo),Jf}var xe=class{},Br=class extends xe{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,oh(n,s=>this.processProvider(s)),this.records.set(ua,jo(void 0,this)),r.has("environment")&&this.records.set(xe,jo(void 0,this));let o=this.records.get(fa);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(Eh,Tt,{self:!0}))}retrieve(n,e){let i=Lr(e)||0;try{return this.get(n,Pr,i)}catch(r){if(Ro(r))return r;throw r}}destroy(){ia(this),this._destroyed=!0;let n=G(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),G(n)}}onDestroy(n){return ia(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){ia(this);let e=Zn(this),i=rn(void 0),r;try{return n()}finally{Zn(e),rn(i)}}get(n,e=Pr,i){if(ia(this),n.hasOwnProperty(a_))return n[a_](this);let r=Lr(i),o,s=Zn(this),a=rn(void 0);try{if(!(r&4)){let l=this.records.get(n);if(l===void 0){let d=kx(n)&&la(n);d&&this.injectableDefInScope(d)?l=jo(rh(n),Tl):l=null,this.records.set(n,l)}if(l!=null)return this.hydrate(n,l,r)}let c=r&2?Ho():this.parent;return e=r&8&&e===Pr?null:e,c.get(n,e)}catch(c){let l=yx(c);throw l===-200||l===-201?new D(l,null):c}finally{rn(a),Zn(s)}}resolveInjectorInitializers(){let n=G(null),e=Zn(this),i=rn(void 0),r;try{let o=this.get(Hr,Tt,{self:!0});for(let s of o)s()}finally{Zn(e),rn(i),G(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=yt(n);let e=jr(n)?n:yt(n&&n.provide),i=Ax(n);if(!jr(n)&&n.multi===!0){let r=this.records.get(e);r||(r=jo(void 0,Tl,!0),r.factory=()=>ih(r.multi),this.records.set(e,r)),e=n,r.multi.push(n)}this.records.set(e,i)}hydrate(n,e,i){let r=G(null);try{if(e.value===c_)throw yh("");return e.value===Tl&&(e.value=c_,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&Nx(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{G(r)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=yt(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function rh(t){let n=la(t),e=n!==null?n.factory:Vr(t);if(e!==null)return e;if(t instanceof y)throw new D(-204,!1);if(t instanceof Function)return Tx(t);throw new D(-204,!1)}function Tx(t){if(t.length>0)throw new D(-204,!1);let e=gx(t);return e!==null?()=>e.factory(t):()=>new t}function Ax(t){if(D_(t))return jo(void 0,t.useValue);{let n=xh(t);return jo(n,Tl)}}function xh(t,n,e){let i;if(jr(t)){let r=yt(t);return Vr(r)||rh(r)}else if(D_(t))i=()=>yt(t.useValue);else if(Mx(t))i=()=>t.useFactory(...ih(t.deps||[]));else if(Sx(t))i=(r,o)=>M(yt(t.useExisting),o!==void 0&&o&8?8:void 0);else{let r=yt(t&&(t.useClass||t.provide));if(Rx(t))i=()=>new r(...ih(t.deps));else return Vr(r)||rh(r)}return i}function ia(t){if(t.destroyed)throw new D(-205,!1)}function jo(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function Rx(t){return!!t.deps}function Nx(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function kx(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function oh(t,n){for(let e of t)Array.isArray(e)?oh(e,n):e&&hh(e)?oh(e.\u0275providers,n):n(e)}function ut(t,n){let e;t instanceof Br?(ia(t),e=t):e=new nh(t);let i,r=Zn(e),o=rn(void 0);try{return n()}finally{Zn(r),rn(o)}}function I_(){return g_()!==void 0||Kc()!=null}var Ln=0,P=1,W=2,dt=3,pn=4,At=5,zo=6,$o=7,bt=8,yi=9,Vn=10,Me=11,Go=12,Ih=13,Wi=14,Xt=15,qi=16,zr=17,Xn=18,bi=19,Sh=20,mi=21,$l=22,Hi=23,on=24,$r=25,Yi=26,Ke=27,S_=1;var Zi=7,ha=8,Gr=9,Ct=10;function Ci(t){return Array.isArray(t)&&typeof t[S_]=="object"}function mn(t){return Array.isArray(t)&&t[S_]===!0}function Mh(t){return(t.flags&4)!==0}function Ei(t){return t.componentOffset>-1}function Wo(t){return(t.flags&1)===1}function Jn(t){return!!t.template}function qo(t){return(t[W]&512)!==0}function Wr(t){return(t[W]&256)===256}var Th="svg",M_="math";function gn(t){for(;Array.isArray(t);)t=t[Ln];return t}function Ah(t,n){return gn(n[t])}function vn(t,n){return gn(n[t.index])}function Gl(t,n){return t.data[n]}function T_(t,n){return t[n]}function _n(t,n){let e=n[t];return Ci(e)?e:e[Ln]}function A_(t){return(t[W]&4)===4}function Wl(t){return(t[W]&128)===128}function R_(t){return mn(t[dt])}function jn(t,n){return n==null?null:t[n]}function Rh(t){t[zr]=0}function Nh(t){t[W]&1024||(t[W]|=1024,Wl(t)&&qr(t))}function N_(t,n){for(;t>0;)n=n[Wi],t--;return n}function pa(t){return!!(t[W]&9216||t[on]?.dirty)}function ql(t){t[Vn].changeDetectionScheduler?.notify(8),t[W]&64&&(t[W]|=1024),pa(t)&&qr(t)}function qr(t){t[Vn].changeDetectionScheduler?.notify(0);let n=gi(t);for(;n!==null&&!(n[W]&8192||(n[W]|=8192,!Wl(n)));)n=gi(n)}function Yl(t,n){if(Wr(t))throw new D(911,!1);t[mi]===null&&(t[mi]=[]),t[mi].push(n)}function k_(t,n){if(t[mi]===null)return;let e=t[mi].indexOf(n);e!==-1&&t[mi].splice(e,1)}function gi(t){let n=t[dt];return mn(n)?n[dt]:n}function kh(t){return t[$o]??=[]}function Oh(t){return t.cleanup??=[]}function O_(t,n,e,i){let r=kh(n);r.push(e),t.firstCreatePass&&Oh(t).push(i,r.length-1)}var se={lFrame:W_(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var sh=!1;function F_(){return se.lFrame.elementDepthCount}function P_(){se.lFrame.elementDepthCount++}function Fh(){se.lFrame.elementDepthCount--}function Zl(){return se.bindingsEnabled}function Ph(){return se.skipHydrationRootTNode!==null}function Lh(t){return se.skipHydrationRootTNode===t}function Vh(){se.skipHydrationRootTNode=null}function Z(){return se.lFrame.lView}function Ae(){return se.lFrame.tView}function le(t){return se.lFrame.contextLView=t,t[bt]}function de(t){return se.lFrame.contextLView=null,t}function Qe(){let t=jh();for(;t!==null&&t.type===64;)t=t.parent;return t}function jh(){return se.lFrame.currentTNode}function L_(){let t=se.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function Yo(t,n){let e=se.lFrame;e.currentTNode=t,e.isParent=n}function Bh(){return se.lFrame.isParent}function Uh(){se.lFrame.isParent=!1}function V_(){return se.lFrame.contextLView}function Hh(){return sh}function oa(t){let n=sh;return sh=t,n}function j_(){let t=se.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function B_(t){return se.lFrame.bindingIndex=t}function Yr(){return se.lFrame.bindingIndex++}function zh(t){let n=se.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function U_(){return se.lFrame.inI18n}function H_(t,n){let e=se.lFrame;e.bindingIndex=e.bindingRootIndex=t,Kl(n)}function z_(){return se.lFrame.currentDirectiveIndex}function Kl(t){se.lFrame.currentDirectiveIndex=t}function $_(t){let n=se.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function Ql(){return se.lFrame.currentQueryIndex}function ma(t){se.lFrame.currentQueryIndex=t}function Ox(t){let n=t[P];return n.type===2?n.declTNode:n.type===1?t[At]:null}function $h(t,n,e){if(e&4){let r=n,o=t;for(;r=r.parent,r===null&&!(e&1);)if(r=Ox(o),r===null||(o=o[Wi],r.type&10))break;if(r===null)return!1;n=r,t=o}let i=se.lFrame=G_();return i.currentTNode=n,i.lView=t,!0}function Xl(t){let n=G_(),e=t[P];se.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function G_(){let t=se.lFrame,n=t===null?null:t.child;return n===null?W_(t):n}function W_(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function q_(){let t=se.lFrame;return se.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var Gh=q_;function Jl(){let t=q_();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function Y_(t){return(se.lFrame.contextLView=N_(t,se.lFrame.contextLView))[bt]}function Ki(){return se.lFrame.selectedIndex}function Qi(t){se.lFrame.selectedIndex=t}function Zo(){let t=se.lFrame;return Gl(t.tView,t.selectedIndex)}function ei(){se.lFrame.currentNamespace=Th}function ed(){Fx()}function Fx(){se.lFrame.currentNamespace=null}function Wh(){return se.lFrame.currentNamespace}var Z_=!0;function td(){return Z_}function ga(t){Z_=t}function ah(t,n=null,e=null,i){let r=qh(t,n,e,i);return r.resolveInjectorInitializers(),r}function qh(t,n=null,e=null,i,r=new Set){let o=[e||Tt,E_(t)],s;return new Br(o,n||Ho(),s||null,r)}var ae=class t{static THROW_IF_NOT_FOUND=Pr;static NULL=new Bo;static create(n,e){if(Array.isArray(n))return ah({name:""},e,n,"");{let i=n.name??"";return ah({name:i},n.parent,n.providers,i)}}static \u0275prov=V({token:t,providedIn:"any",factory:()=>M(ua)});static __NG_ELEMENT_ID__=-1},U=new y(""),Et=(()=>{class t{static __NG_ELEMENT_ID__=Px;static __NG_ENV_ID__=e=>e}return t})(),Nl=class extends Et{_lView;constructor(n){super(),this._lView=n}get destroyed(){return Wr(this._lView)}onDestroy(n){let e=this._lView;return Yl(e,n),()=>k_(e,n)}};function Px(){return new Nl(Z())}var K_=!1,Q_=new y(""),wi=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new st(!1);debugTaskTracker=u(Q_,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new re(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=V({token:t,providedIn:"root",factory:()=>new t})}return t})(),ch=class extends S{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,I_()&&(this.destroyRef=u(Et,{optional:!0})??void 0,this.pendingTasks=u(wi,{optional:!0})??void 0)}emit(n){let e=G(null);try{super.next(n)}finally{G(e)}}subscribe(n,e,i){let r=n,o=e||(()=>null),s=i;if(n&&typeof n=="object"){let c=n;r=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return n instanceof fe&&n.add(a),a}wrapInTimeout(n){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},Y=ch;function kl(...t){}function Yh(t){let n,e;function i(){t=kl;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch(r){}}return n=setTimeout(()=>{t(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),i()})),()=>i()}function X_(t){return queueMicrotask(()=>t()),()=>{t=kl}}var Zh="isAngularZone",sa=Zh+"_ID",Lx=0,O=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new Y(!1);onMicrotaskEmpty=new Y(!1);onStable=new Y(!1);onError=new Y(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=K_}=n;if(typeof Zone>"u")throw new D(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,Bx(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Zh)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new D(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new D(909,!1)}run(n,e,i){return this._inner.run(n,e,i)}runTask(n,e,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,n,Vx,kl,kl);try{return o.runTask(s,e,i)}finally{o.cancelTask(s)}}runGuarded(n,e,i){return this._inner.runGuarded(n,e,i)}runOutsideAngular(n){return this._outer.run(n)}},Vx={};function Kh(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function jx(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){Yh(()=>{t.callbackScheduled=!1,lh(t),t.isCheckStableRunning=!0,Kh(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),lh(t)}function Bx(t){let n=()=>{jx(t)},e=Lx++;t._inner=t._inner.fork({name:"angular",properties:{[Zh]:!0,[sa]:e,[sa+e]:!0},onInvokeTask:(i,r,o,s,a,c)=>{if(Ux(c))return i.invokeTask(o,s,a,c);try{return l_(t),i.invokeTask(o,s,a,c)}finally{(t.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),d_(t)}},onInvoke:(i,r,o,s,a,c,l)=>{try{return l_(t),i.invoke(o,s,a,c,l)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!Hx(c)&&n(),d_(t)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(t._hasPendingMicrotasks=s.microTask,lh(t),Kh(t)):s.change=="macroTask"&&(t.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),t.runOutsideAngular(()=>t.onError.emit(s)),!1)})}function lh(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function l_(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function d_(t){t._nesting--,Kh(t)}var aa=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new Y;onMicrotaskEmpty=new Y;onStable=new Y;onError=new Y;run(n,e,i){return n.apply(e,i)}runGuarded(n,e,i){return n.apply(e,i)}runOutsideAngular(n){return n()}runTask(n,e,i,r){return n.apply(e,i)}};function Ux(t){return J_(t,"__ignore_ng_zone__")}function Hx(t){return J_(t,"__scheduler_tick__")}function J_(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var Wt=class{_console=console;handleError(n){this._console.error("ERROR",n)}},sn=new y("",{factory:()=>{let t=u(O),n=u(xe),e;return i=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw i}):(e??=n.get(Wt),e.handleError(i))})}}}),ey={provide:Hr,useValue:()=>{let t=u(Wt,{optional:!0})},multi:!0},zx=new y("",{factory:()=>{let t=u(U).defaultView;if(!t)return;let n=u(sn),e=o=>{n(o.reason),o.preventDefault()},i=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),u(Et).onDestroy(()=>{t.removeEventListener("error",i),t.removeEventListener("unhandledrejection",e)})}});function Qh(){return _i([C_(()=>{u(zx)})])}function ee(t,n){let[e,i,r]=Of(t,n?.equal),o=e,s=o[qe];return o.set=i,o.update=r,o.asReadonly=nd.bind(o),o}function nd(){let t=this[qe];if(t.readonlyFn===void 0){let n=()=>this();n[qe]=t,t.readonlyFn=n}return t.readonlyFn}var Zr=new y("",{factory:()=>$x}),$x="ng";var id=new y(""),Kr=new y("",{providedIn:"platform",factory:()=>"unknown"}),va=new y(""),Qr=new y("",{factory:()=>u(U).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Ko=(()=>{class t{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=Gx}return t})();function Gx(){return new Ko(Z(),Qe())}var Qn=class{},_a=new y("",{factory:()=>!0});var Xh=new y(""),rd=(()=>{class t{static \u0275prov=V({token:t,providedIn:"root",factory:()=>new dh})}return t})(),dh=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,i=this.queues.get(e);i.has(n)&&(i.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(n)||i.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,i]of this.queues)e===null?n||=this.flushQueue(i):n||=e.run(()=>this.flushQueue(i));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let i of n)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},Ol=class{[qe];constructor(n){this[qe]=n}destroy(){this[qe].destroy()}};function Bn(t,n){let e=n?.injector??u(ae),i=n?.manualCleanup!==!0?e.get(Et):null,r,o=e.get(Ko,null,{optional:!0}),s=e.get(Qn);return o!==null?(r=Yx(o.view,s,t),i instanceof Nl&&i._lView===o.view&&(i=null)):r=Zx(t,e.get(rd),s),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Ol(r)}var ty=H(C({},Ff),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=oa(!1);try{Pf(this)}finally{oa(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=G(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],G(t)}}}),Wx=H(C({},ty),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Ui(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),qx=H(C({},ty),{consumerMarkedDirty(){this.view[W]|=8192,qr(this.view),this.notifier.notify(13)},destroy(){if(Ui(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[Hi]?.delete(this)}});function Yx(t,n,e){let i=Object.create(qx);return i.view=t,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=n,i.fn=ny(i,e),t[Hi]??=new Set,t[Hi].add(i),i.consumerMarkedDirty(i),i}function Zx(t,n,e){let i=Object.create(Wx);return i.fn=ny(i,t),i.scheduler=n,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function ny(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function yn(t){return typeof t=="function"&&t[qe]!==void 0}function od(t){return yn(t)&&typeof t.set=="function"}var ya=(()=>{class t{internalPendingTasks=u(wi);scheduler=u(Qn);errorHandler=u(sn);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();try{e().catch(this.errorHandler).finally(i)}catch(r){this.errorHandler(r),i()}}static \u0275prov=V({token:t,providedIn:"root",factory:()=>new t})}return t})();function Ma(t){return{toString:t}.toString()}var ve=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(ve||{}),pd=class{previousValue;currentValue;firstChange;constructor(n,e,i){this.previousValue=n,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}};function Uy(t,n,e,i){n!==null?n.applyValueToInputSignal(n,i):t[e]=i}var Hy=null,tt=(()=>{Hy=iy;let t=()=>iy;return t.ngInherit=!0,t})();function oI(){return Hy}function iy(t){return t.type.prototype.ngOnChanges&&(t.setInput=aI),sI}function sI(){let t=zy(this),n=t?.current;if(n){let e=t.previous;if(e===Gi)t.previous=n;else for(let i in n)e[i]=n[i];t.current=null,this.ngOnChanges(n)}}function aI(t,n,e,i,r){let o=this.declaredInputs[i],s=zy(t)||cI(t,{previous:Gi,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new pd(l&&l.currentValue,e,c===Gi),Uy(t,n,r,e)}var cp="__ngSimpleChanges__";function zy(t){return Object.hasOwn(t,cp)&&t[cp]||null}function cI(t,n){return t[cp]=n}var ry=[];var we=function(t,n=null,e){for(let i=0;i<ry.length;i++){let r=ry[i];r(t,n,e)}};function lI(t,n,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=n.type.prototype;if(i){let s=oI()(n);(e.preOrderHooks??=[]).push(t,s),(e.preOrderCheckHooks??=[]).push(t,s)}r&&(e.preOrderHooks??=[]).push(0-t,r),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function $y(t,n){for(let e=n.directiveStart,i=n.directiveEnd;e<i;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:d}=o;s&&(t.contentHooks??=[]).push(-e,s),a&&((t.contentHooks??=[]).push(e,a),(t.contentCheckHooks??=[]).push(e,a)),c&&(t.viewHooks??=[]).push(-e,c),l&&((t.viewHooks??=[]).push(e,l),(t.viewCheckHooks??=[]).push(e,l)),d!=null&&(t.destroyHooks??=[]).push(e,d)}}function dd(t,n,e){Gy(t,n,3,e)}function ud(t,n,e,i){(t[W]&3)===e&&Gy(t,n,e,i)}function Jh(t,n){let e=t[W];(e&3)===n&&(e&=16383,e+=1,t[W]=e)}function Gy(t,n,e,i){let r=i!==void 0?t[zr]&65535:0,o=i??-1,s=n.length-1,a=0;for(let c=r;c<s;c++)if(typeof n[c+1]=="number"){if(a=n[c],i!=null&&a>=i)break}else n[c]<0&&(t[zr]+=65536),(a<o||o==-1)&&(dI(t,e,n,c),t[zr]=(t[zr]&4294901760)+c+2),c++}function oy(t,n){we(ve.LifecycleHookStart,t,n);let e=G(null);try{n.call(t)}finally{G(e),we(ve.LifecycleHookEnd,t,n)}}function dI(t,n,e,i){let r=e[i]<0,o=e[i+1],s=r?-e[i]:e[i],a=t[s];r?t[W]>>14<t[zr]>>16&&(t[W]&3)===n&&(t[W]+=16384,oy(a,o)):oy(a,o)}var Xo=-1,Jr=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,i,r){this.factory=n,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function uI(t){return(t.flags&8)!==0}function fI(t){return(t.flags&16)!==0}function hI(t,n,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],s=e[i++],a=e[i++];t.setAttribute(n,s,a,o)}else{let o=r,s=e[++i];pI(o)?t.setProperty(n,o,s):t.setAttribute(n,o,s),i++}}return i}function Wy(t){return t===3||t===4||t===6}function pI(t){return t.charCodeAt(0)===64}function Jo(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let i=0;i<n.length;i++){let r=n[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?sy(t,e,r,null,n[++i]):sy(t,e,r,null,null))}}return t}function sy(t,n,e,i,r){let o=0,s=t.length;if(n===-1)s=-1;else for(;o<t.length;){let a=t[o++];if(typeof a=="number"){if(a===n){s=-1;break}else if(a>n){s=o-1;break}}}for(;o<t.length;){let a=t[o];if(typeof a=="number")break;if(a===e){r!==null&&(t[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(t.splice(s,0,n),o=s+1),t.splice(o++,0,e),r!==null&&t.splice(o++,0,r)}function qy(t){return t!==Xo}function md(t){return t&32767}function mI(t){return t>>16}function gd(t,n){let e=mI(t),i=n;for(;e>0;)i=i[Wi],e--;return i}var lp=!0;function ay(t){let n=lp;return lp=t,n}var gI=256,Yy=gI-1,Zy=5,vI=0,ti={};function _I(t,n,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(Ur)&&(i=e[Ur]),i==null&&(i=e[Ur]=vI++);let r=i&Yy,o=1<<r;n.data[t+(r>>Zy)]|=o}function vd(t,n){let e=Ky(t,n);if(e!==-1)return e;let i=n[P];i.firstCreatePass&&(t.injectorIndex=n.length,ep(i.data,t),ep(n,null),ep(i.blueprint,null));let r=Vp(t,n),o=t.injectorIndex;if(qy(r)){let s=md(r),a=gd(r,n),c=a[P].data;for(let l=0;l<8;l++)n[o+l]=a[s+l]|c[s+l]}return n[o+8]=r,o}function ep(t,n){t.push(0,0,0,0,0,0,0,0,n)}function Ky(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function Vp(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,i=null,r=n;for(;r!==null;){if(i=tb(r),i===null)return Xo;if(e++,r=r[Wi],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return Xo}function dp(t,n,e){_I(t,n,e)}function yI(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(Wy(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===n)return e[r+1];r=r+2}}}return null}function Qy(t,n,e){if(e&8||t!==void 0)return t;Bl(n,"NodeInjector")}function Xy(t,n,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=t[yi],o=rn(void 0);try{return r?r.get(n,i,e&8):bh(n,i,e&8)}finally{rn(o)}}return Qy(i,n,e)}function Jy(t,n,e,i=0,r){if(t!==null){if(n[W]&2048&&!(i&2)){let s=wI(t,n,e,i,ti);if(s!==ti)return s}let o=eb(t,n,e,i,ti);if(o!==ti)return o}return Xy(n,e,i,r)}function eb(t,n,e,i,r){let o=CI(e);if(typeof o=="function"){if(!$h(n,t,i))return i&1?Qy(r,e,i):Xy(n,e,i,r);try{let s;if(s=o(i),s==null&&!(i&8))Bl(e);else return s}finally{Gh()}}else if(typeof o=="number"){let s=null,a=Ky(t,n),c=Xo,l=i&1?n[Xt][At]:null;for((a===-1||i&4)&&(c=a===-1?Vp(t,n):n[a+8],c===Xo||!ly(i,!1)?a=-1:(s=n[P],a=md(c),n=gd(c,n)));a!==-1;){let d=n[P];if(cy(o,a,d.data)){let f=bI(a,n,e,s,i,l);if(f!==ti)return f}c=n[a+8],c!==Xo&&ly(i,n[P].data[a+8]===l)&&cy(o,a,n)?(s=d,a=md(c),n=gd(c,n)):a=-1}}return r}function bI(t,n,e,i,r,o){let s=n[P],a=s.data[t+8],c=i==null?Ei(a)&&lp:i!=s&&(a.type&3)!==0,l=r&1&&o===a,d=fd(a,s,e,c,l);return d!==null?wa(n,s,d,a,r):ti}function fd(t,n,e,i,r){let o=t.providerIndexes,s=n.data,a=o&1048575,c=t.directiveStart,l=t.directiveEnd,d=o>>20,f=i?a:a+d,m=r?a+d:l;for(let g=f;g<m;g++){let b=s[g];if(g<c&&e===b||g>=c&&b.type===e)return g}if(r){let g=s[c];if(g&&Jn(g)&&g.type===e)return c}return null}function wa(t,n,e,i,r){let o=t[e],s=n.data;if(o instanceof Jr){let a=o;if(a.resolving)throw yh("");let c=ay(a.canSeeViewProviders);a.resolving=!0;let l=s[e].type||s[e],d,f=a.injectImpl?rn(a.injectImpl):null,m=$h(t,i,0);try{o=t[e]=a.factory(void 0,r,s,t,i),n.firstCreatePass&&e>=i.directiveStart&&lI(e,s[e],n)}finally{f!==null&&rn(f),ay(c),a.resolving=!1,Gh()}}return o}function CI(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=t.hasOwnProperty(Ur)?t[Ur]:void 0;return typeof n=="number"?n>=0?n&Yy:EI:n}function cy(t,n,e){let i=1<<t;return!!(e[n+(t>>Zy)]&i)}function ly(t,n){return!(t&2)&&!(t&1&&n)}var Xi=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,i){return Jy(this._tNode,this._lView,n,Lr(i),e)}};function EI(){return new Xi(Qe(),Z())}function Gn(t){return Ma(()=>{let n=t.prototype.constructor,e=n[ra]||up(n),i=Object.prototype,r=Object.getPrototypeOf(t.prototype).constructor;for(;r&&r!==i;){let o=r[ra]||up(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function up(t){return uh(t)?()=>{let n=up(yt(t));return n&&n()}:Vr(t)}function wI(t,n,e,i,r){let o=t,s=n;for(;o!==null&&s!==null&&s[W]&2048&&!qo(s);){let a=eb(o,s,e,i|2,ti);if(a!==ti)return a;let c=o.parent;if(!c){let l=s[Sh];if(l){let d=l.get(e,ti,i&-5);if(d!==ti)return d}c=tb(s),s=s[Wi]}o=c}return r}function tb(t){let n=t[P],e=n.type;return e===2?n.declTNode:e===1?t[At]:null}function Ta(t){return yI(Qe(),t)}function T(t){return{token:t.token,providedIn:t.autoProvided===!1?null:"root",factory:t.factory,value:void 0}}function DI(){return is(Qe(),Z())}function is(t,n){return new j(vn(t,n))}var j=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=DI}return t})();function nb(t){return t instanceof j?t.nativeElement:t}function xI(){return this._results[Symbol.iterator]()}var zn=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new S}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let i=__(n);(this._changesDetected=!v_(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=xI};function ib(t){return(t.flags&128)===128}var jp=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(jp||{}),rb=new Map,II=0;function SI(){return II++}function MI(t){rb.set(t[bi],t)}function fp(t){rb.delete(t[bi])}var dy="__ngContext__";function es(t,n){Ci(n)?(t[dy]=n[bi],MI(n)):t[dy]=n}function ob(t){return ab(t[Go])}function sb(t){return ab(t[pn])}function ab(t){for(;t!==null&&!mn(t);)t=t[pn];return t}var hp;function Bp(t){hp=t}function cb(){if(hp!==void 0)return hp;if(typeof document<"u")return document;throw new D(210,!1)}var lb=!1,db=new y("",{factory:()=>lb});var uy=new WeakMap;function TI(t,n){if(t==null||typeof t!="object")return;let e=uy.get(t);e||(e=new WeakSet,uy.set(t,e)),e.add(n)}var AI=(t,n,e,i)=>{};function RI(t,n,e,i){AI(t,n,e,i)}function Ad(t){return(t.flags&32)===32}var NI=()=>null;function ub(t,n,e=!1){return NI(t,n,e)}function fb(t,n){let e=t.contentQueries;if(e!==null){let i=G(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],s=e[r+1];if(s!==-1){let a=t.data[s];ma(o),a.contentQueries(2,n[s],s)}}}finally{G(i)}}}function pp(t,n,e){ma(0);let i=G(null);try{n(t,e)}finally{G(i)}}function Up(t,n,e){if(Mh(n)){let i=G(null);try{let r=n.directiveStart,o=n.directiveEnd;for(let s=r;s<o;s++){let a=t.data[s];if(a.contentQueries){let c=e[s];a.contentQueries(1,c,s)}}}finally{G(i)}}}var $n=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})($n||{});var sd;function kI(){if(sd===void 0&&(sd=null,$i.trustedTypes))try{sd=$i.trustedTypes.createPolicy("angular",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch(t){}return sd}function Rd(t){return kI()?.createHTML(t)||t}var ad;function OI(){if(ad===void 0&&(ad=null,$i.trustedTypes))try{ad=$i.trustedTypes.createPolicy("angular#unsafe-bypass",{createHTML:t=>t,createScript:t=>t,createScriptURL:t=>t})}catch(t){}return ad}function fy(t){return OI()?.createScriptURL(t)||t}var Di=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Fl})`}},mp=class extends Di{getTypeName(){return"HTML"}},gp=class extends Di{getTypeName(){return"Style"}},vp=class extends Di{getTypeName(){return"Script"}},_p=class extends Di{getTypeName(){return"URL"}},yp=class extends Di{getTypeName(){return"ResourceURL"}};function Wn(t){return t instanceof Di?t.changingThisBreaksApplicationSecurity:t}function Ii(t,n){let e=hb(t);if(e!=null&&e!==n){if(e==="ResourceURL"&&n==="URL")return!0;throw new Error(`Required a safe ${n}, got a ${e} (see ${Fl})`)}return e===n}function hb(t){return t instanceof Di&&t.getTypeName()||null}function Hp(t){return new mp(t)}function zp(t){return new gp(t)}function $p(t){return new vp(t)}function Gp(t){return new _p(t)}function Wp(t){return new yp(t)}function FI(t){let n=new Cp(t);return PI()?new bp(n):n}var bp=class{inertDocumentHelper;constructor(n){this.inertDocumentHelper=n}getInertBodyElement(n){n="<body><remove></remove>"+n;try{let e=new window.DOMParser().parseFromString(Rd(n),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(n):(e.firstChild?.remove(),e)}catch(e){return null}}},Cp=class{defaultDoc;inertDocument;constructor(n){this.defaultDoc=n,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(n){let e=this.inertDocument.createElement("template");return e.innerHTML=Rd(n),e}};function PI(){try{return!!new window.DOMParser().parseFromString(Rd(""),"text/html")}catch(t){return!1}}var LI=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function Aa(t){return t=String(t),t.match(LI)?t:"unsafe:"+t}function Si(t){let n={};for(let e of t.split(","))n[e]=!0;return n}function Ra(...t){let n={};for(let e of t)for(let i in e)e.hasOwnProperty(i)&&(n[i]=!0);return n}var pb=Si("area,br,col,hr,img,wbr"),mb=Si("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),gb=Si("rp,rt"),VI=Ra(gb,mb),jI=Ra(mb,Si("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),BI=Ra(gb,Si("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),hy=Ra(pb,jI,BI,VI),vb=Si("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),UI=Si("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),HI=Si("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),zI=Ra(vb,UI,HI),$I=Si("script,style,template"),Ep=class{sanitizedSomething=!1;buf=[];sanitizeChildren(n){let e=n.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=qI(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=WI(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(n){let e=py(n).toLowerCase();if(!hy.hasOwnProperty(e))return this.sanitizedSomething=!0,!$I.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=n.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!zI.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let c=o.value;vb[a]&&(c=Aa(c)),this.buf.push(" ",s,'="',my(c),'"')}return this.buf.push(">"),!0}endElement(n){let e=py(n).toLowerCase();hy.hasOwnProperty(e)&&!pb.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(n){this.buf.push(my(n))}};function GI(t,n){return(t.compareDocumentPosition(n)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function WI(t){let n=t.nextSibling;if(n&&t!==n.previousSibling)throw _b(n);return n}function qI(t){let n=t.firstChild;if(n&&GI(t,n))throw _b(n);return n}function py(t){let n=t.nodeName;return typeof n=="string"?n:"FORM"}function _b(t){return new Error(`Failed to sanitize html because the element is clobbered: ${t.outerHTML}`)}var YI=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,ZI=/([^\#-~ |!])/g;function my(t){return t.replace(/&/g,"&amp;").replace(YI,function(n){let e=n.charCodeAt(0),i=n.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(ZI,function(n){return"&#"+n.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var cd;function qp(t,n){let e=null;try{cd=cd||FI(t);let i=n?String(n):"";e=cd.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=cd.getInertBodyElement(i)}while(i!==o);let a=new Ep().sanitizeChildren(gy(e)||e);return Rd(a)}finally{if(e){let i=gy(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function gy(t){return"content"in t&&KI(t)?t.content:null}function KI(t){return t.nodeType===Node.ELEMENT_NODE&&t.nodeName==="TEMPLATE"}var QI=/^>|^->|<!--|-->|--!>|<!-$/g,XI=/(<|>)/g,JI="\u200B$1\u200B";function eS(t){return t.replace(QI,n=>n.replace(XI,JI))}function tS(t,n){return t.createText(n)}function nS(t,n,e){t.setValue(n,e)}function iS(t,n){return t.createComment(eS(n))}function yb(t,n,e){return t.createElement(n,e)}function _d(t,n,e,i,r){t.insertBefore(n,e,i,r)}function bb(t,n,e){t.appendChild(n,e)}function vy(t,n,e,i,r){i!==null?_d(t,n,e,i,r):bb(t,n,e)}function rS(t,n,e,i){t.removeChild(null,n,e,i)}function oS(t,n,e){t.setAttribute(n,"style",e)}function sS(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function Cb(t,n,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&hI(t,n,i),r!==null&&sS(t,n,r),o!==null&&oS(t,n,o)}var at=(function(t){return t[t.NONE=0]="NONE",t[t.HTML=1]="HTML",t[t.STYLE=2]="STYLE",t[t.SCRIPT=3]="SCRIPT",t[t.URL=4]="URL",t[t.RESOURCE_URL=5]="RESOURCE_URL",t[t.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",t})(at||{});function Eb(t){let n=Db();return n?n.sanitize(at.URL,t)||"":Ii(t,"URL")?Wn(t):Aa(jl(t))}function wb(t){let n=Db();if(n)return fy(n.sanitize(at.RESOURCE_URL,t)||"");if(Ii(t,"ResourceURL"))return fy(Wn(t));throw new D(904,!1)}var aS={embed:{src:!0},frame:{src:!0},iframe:{src:!0},media:{src:!0},base:{href:!0},link:{href:!0},object:{data:!0,codebase:!0}};function cS(t,n){return aS[t.toLowerCase()]?.[n.toLowerCase()]===!0?wb:Eb}function Yp(t,n,e){return cS(n,e)(t)}function Db(){let t=Z();return t&&t[Vn].sanitizer}function lS(t){return t instanceof Function?t():t}function dS(t,n,e){let i=t.length;for(;;){let r=t.indexOf(n,e);if(r===-1)return r;if(r===0||t.charCodeAt(r-1)<=32){let o=n.length;if(r+o===i||t.charCodeAt(r+o)<=32)return r}e=r+1}}var xb="ng-template";function uS(t,n,e,i){let r=0;if(i){for(;r<n.length&&typeof n[r]=="string";r+=2)if(n[r]==="class"&&dS(n[r+1].toLowerCase(),e,0)!==-1)return!0}else if(Zp(t))return!1;if(r=n.indexOf(1,r),r>-1){let o;for(;++r<n.length&&typeof(o=n[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function Zp(t){return t.type===4&&t.value!==xb}function fS(t,n,e){let i=t.type===4&&!e?xb:t.value;return n===i}function hS(t,n,e){let i=4,r=t.attrs,o=r!==null?gS(r):0,s=!1;for(let a=0;a<n.length;a++){let c=n[a];if(typeof c=="number"){if(!s&&!Un(i)&&!Un(c))return!1;if(s&&Un(c))continue;s=!1,i=c|i&1;continue}if(!s)if(i&4){if(i=2|i&1,c!==""&&!fS(t,c,e)||c===""&&n.length===1){if(Un(i))return!1;s=!0}}else if(i&8){if(r===null||!uS(t,r,c,e)){if(Un(i))return!1;s=!0}}else{let l=n[++a],d=pS(c,r,Zp(t),e);if(d===-1){if(Un(i))return!1;s=!0;continue}if(l!==""){let f;if(d>o?f="":f=r[d+1].toLowerCase(),i&2&&l!==f){if(Un(i))return!1;s=!0}}}}return Un(i)||s}function Un(t){return(t&1)===0}function pS(t,n,e,i){if(n===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<n.length;){let s=n[r];if(s===t)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=n[++r];for(;typeof a=="string";)a=n[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return vS(n,t)}function Ib(t,n,e=!1){for(let i=0;i<n.length;i++)if(hS(t,n[i],e))return!0;return!1}function mS(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function gS(t){for(let n=0;n<t.length;n++){let e=t[n];if(Wy(e))return n}return t.length}function vS(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let i=t[e];if(typeof i=="number")return-1;if(i===n)return e;e++}return-1}function _S(t,n){e:for(let e=0;e<n.length;e++){let i=n[e];if(t.length===i.length){for(let r=0;r<t.length;r++)if(t[r]!==i[r])continue e;return!0}}return!1}function _y(t,n){return t?":not("+n.trim()+")":n}function yS(t){let n=t[0],e=1,i=2,r="",o=!1;for(;e<t.length;){let s=t[e];if(typeof s=="string")if(i&2){let a=t[++e];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!Un(s)&&(n+=_y(o,r),r=""),i=s,o=o||!Un(i);e++}return r!==""&&(n+=_y(o,r)),n}function bS(t){return t.map(yS).join(",")}function CS(t){let n=[],e=[],i=1,r=2;for(;i<t.length;){let o=t[i];if(typeof o=="string")r===2?o!==""&&n.push(o,t[++i]):r===8&&e.push(o);else{if(!Un(r))break;r=o}i++}return e.length&&n.push(1,...e),n}var bn={},ni=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(ni||{}),ES;function Kp(t,n){return ES(t,n)}var mz=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var wp=new WeakMap;function Sb(t){return t?t[Wi]??t:null}var ba=new WeakSet;function wS(t,n,e){let i=wp.get(t);if(!i||i.length===0)return;let r=n.parentNode,o=n.previousSibling,s=Sb(e);for(let a=i.length-1;a>=0;a--){let{el:c,declarationView:l}=i[a],d=c.parentNode;c===n?(i.splice(a,1),ba.add(c),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):o&&c===o?(i.splice(a,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c)):d&&r&&d!==r&&(s===null||l===null||s===l)&&(i.splice(a,1),c.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),c.parentNode?.removeChild(c))}}function DS(t,n,e){let i=Sb(e),r=wp.get(t);r?r.some(o=>o.el===n)||r.push({el:n,declarationView:i}):wp.set(t,[{el:n,declarationView:i}])}var eo=new Set,Nd=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(Nd||{}),ii=new y(""),yy=new Set;function er(t){yy.has(t)||(yy.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var kd=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=V({token:t,providedIn:"root",factory:()=>new t})}return t})(),Qp=[0,1,2,3],Xp=(()=>{class t{ngZone=u(O);scheduler=u(Qn);errorHandler=u(Wt,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){u(ii,{optional:!0})}execute(){let e=this.sequences.size>0;e&&we(ve.AfterRenderHooksStart),this.executing=!0;for(let i of Qp)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&we(ve.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[$r]??=[]).push(e),qr(i),i[W]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(Nd.AFTER_NEXT_RENDER,e):e()}static \u0275prov=V({token:t,providedIn:"root",factory:()=>new t})}return t})(),Da=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,i,r,o,s=null){this.impl=n,this.hooks=e,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[$r];n&&(this.view[$r]=n.filter(e=>e!==this))}};function Cn(t,n){let e=n?.injector??u(ae);return er("NgAfterNextRender"),IS(t,e,n,!0)}function xS(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function IS(t,n,e,i){let r=n.get(kd);r.impl??=n.get(Xp);let o=n.get(ii,null,{optional:!0}),s=e?.manualCleanup!==!0?n.get(Et):null,a=n.get(Ko,null,{optional:!0}),c=new Da(r.impl,xS(t),a?.view,i,s,o?.snapshot(null));return r.impl.register(c),c}var Mb=new y("",{factory:()=>{let t=u(xe),n=new Set;return t.onDestroy(()=>n.clear()),{queue:n,isScheduled:!1,scheduler:null,injector:t}}});function Tb(t,n,e){let i=t.get(Mb);if(Array.isArray(n))for(let r of n)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(n),e?.detachedLeaveAnimationFns?.push(n);i.scheduler&&i.scheduler(t)}function SS(t,n){let e=t.get(Mb);if(Array.isArray(n))for(let i of n)e.queue.delete(i);else e.queue.delete(n)}function MS(t,n){for(let[e,i]of n)Tb(t,i.animateFns)}function by(t,n,e,i){let r=t?.[Yi]?.enter;n!==null&&r&&r.has(e.index)&&MS(i,r)}function Cy(t,n,e,i){try{e.get(ua)}catch(s){return i(!1)}let r=t?.[Yi];r?.enter?.has(n.index)&&SS(e,r.enter.get(n.index).animateFns);let o=TS(t,n,r);if(o.size===0){let s=!1;if(t){let a=[];Od(t,n,a),s=a.length>0}if(!s)return i(!1)}t&&eo.add(t[bi]),Tb(e,()=>AS(t,n,r||void 0,o,i),r||void 0)}function TS(t,n,e){let i=new Map,r=e?.leave;if(r&&r.has(n.index)&&i.set(n.index,r.get(n.index)),t&&r)for(let[o,s]of r){if(i.has(o))continue;let c=t[P].data[o].parent;for(;c;){if(c===n){i.set(o,s);break}c=c.parent}}return i}function AS(t,n,e,i,r){let o=[];if(e&&e.leave)for(let[s]of i){if(!e.leave.has(s))continue;let a=e.leave.get(s);for(let c of a.animateFns){let{promise:l}=c();o.push(l)}e.detachedLeaveAnimationFns=void 0}if(t&&Od(t,n,o),o.length>0){let s=e||t?.[Yi];if(s){let a=s.running;a&&o.push(a),s.running=Promise.allSettled(o),NS(t,s.running,r)}else Promise.allSettled(o).then(()=>{t&&eo.delete(t[bi]),r(!0)})}else t&&eo.delete(t[bi]),r(!1)}function Od(t,n,e){if(n.type&12){let r=t[n.index];if(mn(r))for(let o=Ct;o<r.length;o++){let s=r[o];s[P].type===2&&RS(s,e)}}let i=n.child;for(;i;)Od(t,i,e),i=i.next}function RS(t,n){let e=t[Yi];if(e&&e.leave)for(let r of e.leave.values())for(let o of r.animateFns){let{promise:s}=o();n.push(s)}let i=t[P].firstChild;for(;i;)Od(t,i,n),i=i.next}function NS(t,n,e){n.then(()=>{t[Yi]?.running===n&&(t[Yi].running=void 0,eo.delete(t[bi])),e(!0)})}function Qo(t,n,e,i,r,o,s,a){if(r!=null){let c,l=!1;mn(r)?c=r:Ci(r)&&(l=!0,r=r[Ln]);let d=gn(r);t===0&&i!==null?(by(a,i,o,e),s==null?bb(n,i,d):_d(n,i,d,s||null,!0)):t===1&&i!==null?(by(a,i,o,e),_d(n,i,d,s||null,!0),wS(o,d,a)):t===2?(a?.[Yi]?.leave?.has(o.index)&&DS(o,d,a),ba.delete(d),Cy(a,o,e,f=>{if(ba.has(d)){ba.delete(d);return}rS(n,d,l,f)})):t===3&&(ba.delete(d),Cy(a,o,e,()=>{n.destroyNode(d)})),c!=null&&HS(n,t,e,c,o,i,s)}}function kS(t,n){Ab(t,n),n[Ln]=null,n[At]=null}function OS(t,n,e,i,r,o){i[Ln]=r,i[At]=n,Fd(t,i,e,1,r,o)}function Ab(t,n){n[Vn].changeDetectionScheduler?.notify(9),Fd(t,n,n[Me],2,null,null)}function FS(t){let n=t[Go];if(!n)return tp(t[P],t);for(;n;){let e=null;if(Ci(n))e=n[Go];else{let i=n[Ct];i&&(e=i)}if(!e){for(;n&&!n[pn]&&n!==t;)Ci(n)&&tp(n[P],n),n=n[dt];n===null&&(n=t),Ci(n)&&tp(n[P],n),e=n&&n[pn]}n=e}}function Jp(t,n){let e=t[Gr],i=e.indexOf(n);e.splice(i,1)}function em(t,n){if(Wr(n))return;let e=n[Me];e.destroyNode&&Fd(t,n,e,3,null,null),FS(n)}function tp(t,n){if(Wr(n))return;let e=G(null);try{n[W]&=-129,n[W]|=256,n[on]&&Ui(n[on]),LS(t,n),PS(t,n),n[P].type===1&&n[Me].destroy();let i=n[qi];if(i!==null&&mn(n[dt])){i!==n[dt]&&Jp(i,n);let r=n[Xn];r!==null&&r.detachView(t)}fp(n)}finally{G(e)}}function PS(t,n){let e=t.cleanup,i=n[$o];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[e[s+1]];e[s].call(a)}i!==null&&(n[$o]=null);let r=n[mi];if(r!==null){n[mi]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=n[Hi];if(o!==null){n[Hi]=null;for(let s of o)s.destroy()}}function LS(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=n[e[i]];if(!(r instanceof Jr)){let o=e[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],c=o[s+1];we(ve.LifecycleHookStart,a,c);try{c.call(a)}finally{we(ve.LifecycleHookEnd,a,c)}}else{we(ve.LifecycleHookStart,r,o);try{o.call(r)}finally{we(ve.LifecycleHookEnd,r,o)}}}}}function Rb(t,n,e){return VS(t,n.parent,e)}function VS(t,n,e){let i=n;for(;i!==null&&i.type&168;)n=i,i=n.parent;if(i===null)return e[Ln];if(Ei(i)){let{encapsulation:r}=t.data[i.directiveStart+i.componentOffset];if(r===$n.None||r===$n.Emulated)return null}return vn(i,e)}function Nb(t,n,e){return BS(t,n,e)}function jS(t,n,e){return t.type&40?vn(t,e):null}var BS=jS,Ey;function tm(t,n,e,i){let r=Rb(t,i,n),o=n[Me],s=i.parent||n[At],a=Nb(s,i,n);if(r!=null)if(Array.isArray(e))for(let c=0;c<e.length;c++)vy(o,r,e[c],a,!1);else vy(o,r,e,a,!1);Ey!==void 0&&Ey(o,i,n,e,r)}function Ca(t,n){if(n!==null){let e=n.type;if(e&3)return vn(n,t);if(e&4)return Dp(-1,t[n.index]);if(e&8){let i=n.child;if(i!==null)return Ca(t,i);{let r=t[n.index];return mn(r)?Dp(-1,r):gn(r)}}else{if(e&128)return Ca(t,n.next);if(e&32)return Kp(n,t)()||gn(t[n.index]);{let i=kb(t,n);if(i!==null){if(Array.isArray(i))return i[0];let r=gi(t[Xt]);return Ca(r,i)}else return Ca(t,n.next)}}}return null}function kb(t,n){if(n!==null){let i=t[Xt][At],r=n.projection;return i.projection[r]}return null}function Dp(t,n){let e=Ct+t+1;if(e<n.length){let i=n[e],r=i[P].firstChild;if(r!==null)return Ca(i,r)}return n[Zi]}function nm(t,n,e,i,r,o,s){for(;e!=null;){let a=i[yi];if(e.type===128){e=e.next;continue}let c=i[e.index],l=e.type;if(s&&n===0&&(c&&es(gn(c),i),e.flags|=2),!Ad(e))if(l&8)nm(t,n,e.child,i,r,o,!1),Qo(n,t,a,r,c,e,o,i);else if(l&32){let d=Kp(e,i),f;for(;f=d();)Qo(n,t,a,r,f,e,o,i);Qo(n,t,a,r,c,e,o,i)}else l&16?Ob(t,n,i,e,r,o):Qo(n,t,a,r,c,e,o,i);e=s?e.projectionNext:e.next}}function Fd(t,n,e,i,r,o){nm(e,i,t.firstChild,n,r,o,!1)}function US(t,n,e){let i=n[Me],r=Rb(t,e,n),o=e.parent||n[At],s=Nb(o,e,n);Ob(i,0,n,e,r,s)}function Ob(t,n,e,i,r,o){let s=e[Xt],c=s[At].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let d=c[l];Qo(n,t,e[yi],r,d,i,o,e)}else{let l=c,d=s[dt];ib(i)&&(l.flags|=128),nm(t,n,l,d,r,o,!0)}}function HS(t,n,e,i,r,o,s){let a=i[Zi],c=gn(i);a!==c&&Qo(n,t,e,o,a,r,s);for(let l=Ct;l<i.length;l++){let d=i[l];Fd(d[P],d,t,n,o,a)}}function zS(t,n,e,i,r){if(n)r?t.addClass(e,i):t.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:ni.DashCase;r==null?t.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=ni.Important),t.setStyle(e,i,r,o))}}function im(t,n,e,i,r,o,s,a,c,l,d){let f=Ke+i,m=f+r,g=$S(f,m),b=typeof l=="function"?l():l;return g[P]={type:t,blueprint:g,template:e,queries:null,viewQuery:a,declTNode:n,data:g.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:m,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:b,incompleteFirstPass:!1,ssrId:d}}function $S(t,n){let e=[];for(let i=0;i<n;i++)e.push(i<t?null:bn);return e}function GS(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=im(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function rm(t,n,e,i,r,o,s,a,c,l,d){let f=n.blueprint.slice();return f[Ln]=r,f[W]=i|4|128|8|64|1024,(l!==null||t&&t[W]&2048)&&(f[W]|=2048),Rh(f),f[dt]=f[Wi]=t,f[bt]=e,f[Vn]=s||t&&t[Vn],f[Me]=a||t&&t[Me],f[yi]=c||t&&t[yi]||null,f[At]=o,f[bi]=SI(),f[zo]=d,f[Sh]=l,f[Xt]=n.type==2?t[Xt]:f,f}function WS(t,n,e){let i=vn(n,t),r=GS(e),o=t[Vn].rendererFactory,s=om(t,rm(t,r,null,Fb(e),i,n,null,o.createRenderer(i,e),null,null,null));return t[n.index]=s}function Fb(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function Pb(t,n,e,i){if(e===0)return-1;let r=n.length;for(let o=0;o<e;o++)n.push(i),t.blueprint.push(i),t.data.push(null);return r}function om(t,n){return t[Go]?t[Ih][pn]=n:t[Go]=n,t[Ih]=n,n}function v(t=1){Lb(Ae(),Z(),Ki()+t,!1)}function Lb(t,n,e,i){if(!i)if((n[W]&3)===3){let o=t.preOrderCheckHooks;o!==null&&dd(n,o,e)}else{let o=t.preOrderHooks;o!==null&&ud(n,o,0,e)}Qi(e)}var Pd=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(Pd||{});function to(t,n,e,i){let r=G(null);try{let[o,s,a]=t.inputs[e],c=null;(s&Pd.SignalBased)!==0&&(c=n[o][qe]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(n,i)),t.setInput!==null?t.setInput(n,c,i,e,o):Uy(n,c,o,i)}finally{G(r)}}function Vb(t,n,e,i,r){let o=Ki(),s=i&2;try{Qi(-1),s&&n.length>Ke&&Lb(t,n,Ke,!1);let a=s?ve.TemplateUpdateStart:ve.TemplateCreateStart;we(a,r,e),e(i,r)}finally{Qi(o);let a=s?ve.TemplateUpdateEnd:ve.TemplateCreateEnd;we(a,r,e)}}function Ld(t,n,e){XS(t,n,e),(e.flags&64)===64&&JS(t,n,e)}function Na(t,n,e=vn){let i=n.localNames;if(i!==null){let r=n.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?e(n,t):t[s];t[r++]=a}}}function qS(t,n,e,i){let o=i.get(db,lb)||e===$n.ShadowDom||e===$n.ExperimentalIsolatedShadowDom,s=t.selectRootElement(n,o);return YS(s),s}function YS(t){ZS(t)}var ZS=()=>null;function KS(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function jb(t,n,e,i,r,o){let s=n[P];if(lm(t,s,n,e,i)){Ei(t)&&QS(n,t.index);return}t.type&3&&(e=KS(e)),Bb(t,n,e,i,r,o)}function Bb(t,n,e,i,r,o){if(t.type&3){let s=vn(t,n);i=o!=null?o(i,t.value||"",e):i,r.setProperty(s,e,i)}else t.type&12}function QS(t,n){let e=_n(n,t);e[W]&16||(e[W]|=64)}function XS(t,n,e){let i=e.directiveStart,r=e.directiveEnd;Ei(e)&&WS(n,e,t.data[i+e.componentOffset]),t.firstCreatePass||vd(e,n);let o=e.initialInputs;for(let s=i;s<r;s++){let a=t.data[s],c=wa(n,t,s,e);if(es(c,n),o!==null&&iM(n,s-i,c,a,e,o),Jn(a)){let l=_n(e.index,n);l[bt]=wa(n,t,s,e)}}}function JS(t,n,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,s=z_();try{Qi(o);for(let a=i;a<r;a++){let c=t.data[a],l=n[a];Kl(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&eM(c,l)}}finally{Qi(-1),Kl(s)}}function eM(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function sm(t,n){let e=t.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];Ib(n,o.selectors,!1)&&(i??=[],Jn(o)?i.unshift(o):i.push(o))}return i}function tM(t,n,e,i,r,o){let s=vn(t,n);nM(n[Me],s,o,t.value,e,i,r)}function nM(t,n,e,i,r,o,s){if(o==null)s?.(o,i||"",r),t.removeAttribute(n,r,e);else{let a=s==null?jl(o):s(o,i||"",r);t.setAttribute(n,r,a,e)}}function iM(t,n,e,i,r,o){let s=o[n];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];to(i,e,c,l)}}function am(t,n,e,i,r){let o=Ke+e,s=n[P],a=r(s,n,t,i,e);n[o]=a,Yo(t,!0);let c=t.type===2;return c?(Cb(n[Me],a,t),(F_()===0||Wo(t))&&es(a,n),P_()):es(a,n),td()&&(!c||!Ad(t))&&tm(s,n,a,t),t}function cm(t){let n=t;return Bh()?Uh():(n=n.parent,Yo(n,!1)),n}function rM(t,n){let e=t[yi];if(!e)return;let i;try{i=e.get(sn,null)}catch(r){i=null}i?.(n)}function lm(t,n,e,i,r){let o=t.inputs?.[i],s=t.hostDirectiveInputs?.[i],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],d=s[c+1],f=n.data[l];to(f,e[l],d,r),a=!0}if(o)for(let c of o){let l=e[c],d=n.data[c];to(d,l,i,r),a=!0}return a}function oM(t,n,e,i,r,o){let s=null,a=null,c=null,l=!1,d=t.directiveToIndex.get(i.type);if(typeof d=="number"?s=d:[s,a,c]=d,a!==null&&c!==null&&t.hostDirectiveInputs?.hasOwnProperty(r)){let f=t.hostDirectiveInputs[r];for(let m=0;m<f.length;m+=2){let g=f[m];if(g>=a&&g<=c){let b=n.data[g],I=f[m+1];to(b,e[g],I,o),l=!0}else if(g>c)break}}return s!==null&&i.inputs.hasOwnProperty(r)&&(to(i,e[s],r,o),l=!0),l}function sM(t,n){let e=_n(n,t),i=e[P];aM(i,e);let r=e[Ln];r!==null&&e[zo]===null&&(e[zo]=ub(r,e[yi])),we(ve.ComponentStart);try{dm(i,e,e[bt])}finally{we(ve.ComponentEnd,e[bt])}}function aM(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function dm(t,n,e){Xl(n);try{let i=t.viewQuery;i!==null&&pp(1,i,e);let r=t.template;r!==null&&Vb(t,n,r,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[Xn]?.finishViewCreation(t),t.staticContentQueries&&fb(t,n),t.staticViewQueries&&pp(2,t.viewQuery,e);let o=t.components;o!==null&&cM(n,o)}catch(i){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),i}finally{n[W]&=-5,Jl()}}function cM(t,n){for(let e=0;e<n.length;e++)sM(t,n[e])}function um(t,n,e,i){let r=G(null);try{let o=n.tView,a=t[W]&4096?4096:16,c=rm(t,o,e,a,null,n,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=t[n.index];c[qi]=l;let d=t[Xn];return d!==null&&(c[Xn]=d.createEmbeddedView(o)),dm(o,c,e),c}finally{G(r)}}function yd(t,n){return!n||n.firstChild===null||ib(t)}function xa(t,n,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=n[e.index];o!==null&&i.push(gn(o)),mn(o)&&Ub(o,i);let s=e.type;if(s&8)xa(t,n,e.child,i);else if(s&32){let a=Kp(e,n),c;for(;c=a();)i.push(c)}else if(s&16){let a=kb(n,e);if(Array.isArray(a))i.push(...a);else{let c=gi(n[Xt]);xa(c[P],c,a,i,!0)}}e=r?e.projectionNext:e.next}return i}function Ub(t,n){for(let e=Ct;e<t.length;e++){let i=t[e],r=i[P].firstChild;r!==null&&xa(i[P],i,r,n)}t[Zi]!==t[Ln]&&n.push(t[Zi])}function Hb(t){if(t[$r]!==null){for(let n of t[$r])n.impl.addSequence(n);t[$r].length=0}}var zb=[];function lM(t){return t[on]??dM(t)}function dM(t){let n=zb.pop()??Object.create(fM);return n.lView=t,n}function uM(t){t.lView[on]!==t&&(t.lView=null,zb.push(t))}var fM=H(C({},Vi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{qr(t.lView)},consumerOnSignalRead(){this.lView[on]=this}});function hM(t){let n=t[on]??Object.create(pM);return n.lView=t,n}var pM=H(C({},Vi),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=gi(t.lView);for(;n&&!$b(n[P]);)n=gi(n);n&&Nh(n)},consumerOnSignalRead(){this.lView[on]=this}});function $b(t){return t.type!==2}function Gb(t){if(t[Hi]===null)return;let n=!0;for(;n;){let e=!1;for(let i of t[Hi])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));n=e&&!!(t[W]&8192)}}var mM=100;function Wb(t,n=0){let i=t[Vn].rendererFactory,r=!1;r||i.begin?.();try{gM(t,n)}finally{r||i.end?.()}}function gM(t,n){let e=Hh();try{oa(!0),xp(t,n);let i=0;for(;pa(t);){if(i===mM)throw new D(103,!1);i++,xp(t,1)}}finally{oa(e)}}function vM(t,n,e,i){if(Wr(n))return;let r=n[W],o=!1,s=!1;Xl(n);let a=!0,c=null,l=null;o||($b(t)?(l=lM(n),c=fi(l)):Yc()===null?(a=!1,l=hM(n),c=fi(l)):n[on]&&(Ui(n[on]),n[on]=null));try{Rh(n),B_(t.bindingStartIndex),e!==null&&Vb(t,n,e,2,i);let d=(r&3)===3;if(!o)if(d){let g=t.preOrderCheckHooks;g!==null&&dd(n,g,null)}else{let g=t.preOrderHooks;g!==null&&ud(n,g,0,null),Jh(n,0)}if(s||_M(n),Gb(n),qb(n,0),t.contentQueries!==null&&fb(t,n),!o)if(d){let g=t.contentCheckHooks;g!==null&&dd(n,g)}else{let g=t.contentHooks;g!==null&&ud(n,g,1),Jh(n,1)}bM(t,n);let f=t.components;f!==null&&Zb(n,f,0);let m=t.viewQuery;if(m!==null&&pp(2,m,i),!o)if(d){let g=t.viewCheckHooks;g!==null&&dd(n,g)}else{let g=t.viewHooks;g!==null&&ud(n,g,2),Jh(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[$l]){for(let g of n[$l])g();n[$l]=null}o||(Hb(n),n[W]&=-73)}catch(d){throw o||qr(n),d}finally{l!==null&&(Bi(l,c),a&&uM(l)),Jl()}}function qb(t,n){for(let e=ob(t);e!==null;e=sb(e))for(let i=Ct;i<e.length;i++){let r=e[i];Yb(r,n)}}function _M(t){for(let n=ob(t);n!==null;n=sb(n)){if(!(n[W]&2))continue;let e=n[Gr];for(let i=0;i<e.length;i++){let r=e[i];Nh(r)}}}function yM(t,n,e){we(ve.ComponentStart);let i=_n(n,t);try{Yb(i,e)}finally{we(ve.ComponentEnd,i[bt])}}function Yb(t,n){Wl(t)&&xp(t,n)}function xp(t,n){let i=t[P],r=t[W],o=t[on],s=!!(n===0&&r&16);if(s||=!!(r&64&&n===0),s||=!!(r&1024),s||=!!(o?.dirty&&To(o)),s||=!1,o&&(o.dirty=!1),t[W]&=-9217,s)vM(i,t,i.template,t[bt]);else if(r&8192){let a=G(null);try{Gb(t),qb(t,1);let c=i.components;c!==null&&Zb(t,c,1),Hb(t)}finally{G(a)}}}function Zb(t,n,e){for(let i=0;i<n.length;i++)yM(t,n[i],e)}function bM(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)Qi(~r);else{let o=r,s=e[++i],a=e[++i];H_(s,o);let c=n[o];we(ve.HostBindingsUpdateStart,c);try{a(2,c)}finally{we(ve.HostBindingsUpdateEnd,c)}}}}finally{Qi(-1)}}function fm(t,n){let e=Hh()?64:1088;for(t[Vn].changeDetectionScheduler?.notify(n);t;){t[W]|=e;let i=gi(t);if(qo(t)&&!i)return t;t=i}return null}function Kb(t,n,e,i){return[t,!0,0,n,null,i,null,e,null,null]}function CM(t,n){let e=Ct+n;if(e<t.length)return t[e]}function hm(t,n,e,i=!0){let r=n[P];if(wM(r,n,t,e),i){let s=Dp(e,t),a=n[Me],c=a.parentNode(t[Zi]);c!==null&&OS(r,t[At],a,n,c,s)}let o=n[zo];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function EM(t,n){let e=bd(t,n);return e!==void 0&&em(e[P],e),e}function bd(t,n){if(t.length<=Ct)return;let e=Ct+n,i=t[e];if(i){let r=i[qi];r!==null&&r!==t&&Jp(r,i),n>0&&(t[e-1][pn]=i[pn]);let o=da(t,Ct+n);kS(i[P],i);let s=o[Xn];s!==null&&s.detachView(o[P]),i[dt]=null,i[pn]=null,i[W]&=-129}return i}function wM(t,n,e,i){let r=Ct+i,o=e.length;i>0&&(e[r-1][pn]=n),i<o-Ct?(n[pn]=e[r],Ch(e,Ct+i,n)):(e.push(n),n[pn]=null),n[dt]=e;let s=n[qi];s!==null&&e!==s&&Qb(s,n);let a=n[Xn];a!==null&&a.insertView(t),ql(n),n[W]|=128}function Qb(t,n){let e=t[Gr],i=n[dt];if(Ci(i))t[W]|=2;else{let r=i[dt][Xt];n[Xt]!==r&&(t[W]|=2)}e===null?t[Gr]=[n]:e.push(n)}var Ji=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[P];return xa(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[bt]}set context(n){this._lView[bt]=n}get destroyed(){return Wr(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[dt];if(mn(n)){let e=n[ha],i=e?e.indexOf(this):-1;i>-1&&(bd(n,i),da(e,i))}this._attachedToViewContainer=!1}em(this._lView[P],this._lView)}onDestroy(n){Yl(this._lView,n)}markForCheck(){fm(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[W]&=-129}reattach(){ql(this._lView),this._lView[W]|=128}detectChanges(){this._lView[W]|=1024,Wb(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new D(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=qo(this._lView),e=this._lView[qi];e!==null&&!n&&Jp(e,this._lView),Ab(this._lView[P],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new D(902,!1);this._appRef=n;let e=qo(this._lView),i=this._lView[qi];i!==null&&!e&&Qb(i,this._lView),ql(this._lView)}};var Jt=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=DM;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=um(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new Ji(o)}}return t})();function DM(){return Vd(Qe(),Z())}function Vd(t,n){return t.type&4?new Jt(n,t,is(t,n)):null}function rs(t,n,e,i,r){let o=t.data[n];if(o===null)o=xM(t,n,e,i,r),U_()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let s=L_();o.injectorIndex=s===null?-1:s.injectorIndex}return Yo(o,!0),o}function xM(t,n,e,i,r){let o=jh(),s=Bh(),a=s?o:o&&o.parent,c=t.data[n]=SM(t,a,e,n,i,r);return IM(t,c,o,s),c}function IM(t,n,e,i){t.firstChild===null&&(t.firstChild=n),e!==null&&(i?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function SM(t,n,e,i,r,o){let s=n?n.injectorIndex:-1,a=0;return Ph()&&(a|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:Wh(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var MM=()=>null,TM=()=>null;function Ip(t,n){return MM(t,n)}function AM(t,n,e){return TM(t,n,e)}var Xb=class{},et=class{},je=(()=>{class t{destroyNode=null;static __NG_ELEMENT_ID__=()=>RM()}return t})();function RM(){let t=Z(),n=Qe(),e=_n(n.index,t);return(Ci(e)?e:t)[Me]}var Jb=(()=>{class t{static \u0275prov=V({token:t,providedIn:"root",factory:()=>null})}return t})();function e0(t){return t.debugInfo?.className||t.type.name||null}var hd={},Cd=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,i){let r=this.injector.get(n,hd,i);return r!==hd||e===hd?r:this.parentInjector.get(n,e,i)}};function pm(t){return n0(t)?Array.isArray(t)||!(t instanceof Map)&&Symbol.iterator in t:!1}function t0(t,n){if(Array.isArray(t))for(let e=0;e<t.length;e++)n(t[e]);else{let e=t[Symbol.iterator](),i;for(;!(i=e.next()).done;)n(i.value)}}function n0(t){return t!==null&&(typeof t=="function"||typeof t=="object")}function NM(t,n,e){return t[n]=e}function Mi(t,n,e){if(e===bn)return!1;let i=t[n];return Object.is(i,e)?!1:(t[n]=e,!0)}function Xr(t,n,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&TI(r,o);let s=Ei(t)?_n(t.index,n):n;fm(s,5);let a=n[bt],c=wy(n,a,e,r),l=i.__ngNextListenerFn__;for(;l;)c=wy(n,a,l,r)&&c,l=l.__ngNextListenerFn__;return c}}function wy(t,n,e,i){let r=G(null);try{return we(ve.OutputStart,n,e),e(i)!==!1}catch(o){return rM(t,o),!1}finally{we(ve.OutputEnd,n,e),G(r)}}function mm(t,n,e,i,r,o,s,a){let c=Wo(t),l=!1,d=null;if(!i&&c&&(d=OM(n,e,o,t.index)),d!==null){let f=d.__ngLastListenerFn__||d;f.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,l=!0}else{let f=vn(t,e),m=i?i(f):f;RI(e,m,o,a),i||(a.__ngNativeEl__=f);let g=r.listen(m,o,a);if(!kM(o)){let b=i?I=>i(gn(I[t.index])):t.index;i0(b,n,e,o,a,g,!1)}}return l}function kM(t){return t.startsWith("animation")||t.startsWith("transition")}function OM(t,n,e,i){let r=t.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===e&&r[o+1]===i){let a=n[$o],c=r[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function i0(t,n,e,i,r,o,s){let a=n.firstCreatePass?Oh(n):null,c=kh(e),l=c.length;c.push(r,o),a&&a.push(i,t,l,(l+1)*(s?-1:1))}function Dy(t,n,e,i,r){let o=null,s=null,a=null,c=!1,l=t.directiveToIndex.get(e.type);if(typeof l=="number"?o=l:[o,s,a]=l,s!==null&&a!==null&&t.hostDirectiveOutputs?.hasOwnProperty(i)){let d=t.hostDirectiveOutputs[i];for(let f=0;f<d.length;f+=2){let m=d[f];if(m>=s&&m<=a)c=!0,Ed(t,n,m,d[f+1],i,r);else if(m>a)break}}return e.outputs.hasOwnProperty(i)&&(c=!0,Ed(t,n,o,i,i,r)),c}function Ed(t,n,e,i,r,o){let s=n[e],a=n[P],l=a.data[e].outputs[i],f=s[l].subscribe(o);i0(t.index,a,n,r,o,f,!0)}function Oe(){FM()}function FM(){let t=Z(),n=Ae(),e=Qe();if(n.firstCreatePass&&LM(n,e),e.controlDirectiveIndex===-1)return;er("NgSignalForms");let i=t[e.controlDirectiveIndex];n.data[e.controlDirectiveIndex].controlDef.create(i,new wd(t,n,e))}function Fe(){PM()}function PM(){let t=Z(),n=Ae(),e=Zo();if(e.controlDirectiveIndex===-1)return;let i=n.data[e.controlDirectiveIndex].controlDef,r=t[e.controlDirectiveIndex];i.update(r,new wd(t,n,e))}var wd=class{lView;tView;tNode;hasPassThrough;constructor(n,e,i){this.lView=n,this.tView=e,this.tNode=i,this.hasPassThrough=!!(i.flags&4096)}get customControl(){return this.tNode.customControlIndex!==-1?this.lView[this.tNode.customControlIndex]:void 0}get nativeElement(){return vn(this.tNode,this.lView)}get descriptor(){return`<${this.tNode.value}>`}listenToCustomControlOutput(n,e){let i=this.tView.data[this.tNode.customControlIndex];Dy(this.tNode,this.lView,i,n,Xr(this.tNode,this.lView,e))}listenToCustomControlModel(n){let e=this.tNode.flags&1024?"valueChange":"checkedChange",i=this.tView.data[this.tNode.customControlIndex];Dy(this.tNode,this.lView,i,e,Xr(this.tNode,this.lView,n))}listenToDom(n,e){mm(this.tNode,this.tView,this.lView,void 0,this.lView[Me],n,e,Xr(this.tNode,this.lView,e))}setInputOnDirectives(n,e){let i=this.tNode.inputs?.[n],r=this.tNode.hostDirectiveInputs?.[n];if(!i&&!r)return!1;let o=!1;if(i)for(let s of i){if(s===this.tNode.controlDirectiveIndex)continue;let a=this.tView.data[s],c=this.lView[s];to(a,c,n,e),o=!0}if(r)for(let s=0;s<r.length;s+=2){let a=r[s];if(a===this.tNode.controlDirectiveIndex)continue;let c=r[s+1],l=this.tView.data[a],d=this.lView[a];to(l,d,c,e),o=!0}return o}setCustomControlModelInput(n){let e=this.tView.data[this.tNode.customControlIndex],i=this.tNode.flags&1024?"value":"checked";oM(this.tNode,this.tView,this.lView,e,i,n)}customControlHasInput(n){if(this.tNode.customControlIndex===-1)return!1;let e=this.tView.data[this.tNode.customControlIndex];return(e.signalFormsInputPresence??=this._buildCustomControlInputCache(e))[n]===!0}_buildCustomControlInputCache(n){let e={};for(let i in n.inputs)e[i]=!0;if(n.hostDirectives!==null){let i=[...n.hostDirectives];for(;i.length>0;){let r=i.shift();if(typeof r!="function"){for(let s in r.inputs)e[r.inputs[s]]=!0;let o=xy(r.directive);o!==null&&i.push(...o);continue}for(let o of r()){if(typeof o=="function")continue;if(o.inputs)for(let a=0;a<o.inputs.length;a+=2){let c=o.inputs[a+1]||o.inputs[a];e[c]=!0}let s=xy(o.directive);s!==null&&i.push(...s)}}}return e}};function xy(t){return typeof t=="function"&&"\u0275dir"in t?t.\u0275dir.hostDirectives??null:null}function LM(t,n,e){for(let r=n.directiveStart;r<n.directiveEnd;r++)if(t.data[r].controlDef){n.controlDirectiveIndex=r;break}if(n.controlDirectiveIndex===-1)return;let i=t.data[n.controlDirectiveIndex].controlDef;if(i.passThroughInput&&(n.inputs?.[i.passThroughInput]?.length??0)>1){n.flags|=4096;return}VM(t,n)}function VM(t,n){for(let e=n.directiveStart;e<n.directiveEnd;e++){let i=t.data[e];if(!(n.directiveToIndex&&!n.directiveToIndex.has(i.type))){if(Iy(i,"value")){n.flags|=1024,n.customControlIndex=e;return}if(Iy(i,"checked")){n.flags|=2048,n.customControlIndex=e;return}}}if(n.hostDirectiveInputs!==null&&n.hostDirectiveOutputs!==null&&n.directiveToIndex!==null){let e=(i,r)=>{let o=n.hostDirectiveInputs[i],s=n.hostDirectiveOutputs[i+"Change"];if(!o||!s)return!1;for(let a=0;a<o.length;a+=2){let c=o[a];for(let l=0;l<s.length;l+=2){let d=s[l];if(c===d)for(let f of n.directiveToIndex.values()){if(!Array.isArray(f))continue;let[m,g,b]=f;if(c>=g&&c<=b)return n.flags|=r,n.customControlIndex=m,!0}}}return!1};if(e("value",1024)||e("checked",2048))return}}function Iy(t,n){return jM(t,n)&&BM(t,n+"Change")}function jM(t,n){return n in t.inputs}function BM(t,n){return n in t.outputs}var Sp=Symbol("BINDING");var io=new y("");function Dd(t,n,e){let i=e?t.styles:null,r=e?t.classes:null,o=0;if(n!==null)for(let s=0;s<n.length;s++){let a=n[s];if(typeof a=="number")o=a;else if(o==1)r=Pl(r,a);else if(o==2){let c=a,l=n[++s];i=Pl(i,c+": "+l+";")}}e?t.styles=i:t.stylesWithoutHost=i,e?t.classes=r:t.classesWithoutHost=r}function x(t,n=0){let e=Z();if(e===null)return M(t,n);let i=Qe();return Jy(i,e,yt(t),n)}function ka(){let t="invalid";throw new Error(t)}function r0(t,n,e,i,r){let o=i===null?null:{"":-1},s=r(t,e);if(s!==null){let a=s,c=null,l=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,c,l]=d.resolveHostDirectives(s);break}zM(t,n,e,a,o,c,l)}o!==null&&i!==null&&UM(e,i,o)}function UM(t,n,e){let i=t.localNames=[];for(let r=0;r<n.length;r+=2){let o=e[n[r+1]];if(o==null)throw new D(-301,!1);i.push(n[r],o)}}function HM(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function zM(t,n,e,i,r,o,s){let a=i.length,c=null;for(let m=0;m<a;m++){let g=i[m];c===null&&Jn(g)&&(c=g,HM(t,e,m)),dp(vd(e,n),t,g.type)}ZM(e,t.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let m=0;m<a;m++){let g=i[m];g.providersResolver&&g.providersResolver(g)}let l=!1,d=!1,f=Pb(t,n,a,null);a>0&&(e.directiveToIndex=new Map);for(let m=0;m<a;m++){let g=i[m];if(e.mergedAttrs=Jo(e.mergedAttrs,g.hostAttrs),GM(t,e,n,f,g),YM(f,g,r),s!==null&&s.has(g)){let[I,N]=s.get(g);e.directiveToIndex.set(g.type,[f,I+e.directiveStart,N+e.directiveStart])}else(o===null||!o.has(g))&&e.directiveToIndex.set(g.type,f);g.contentQueries!==null&&(e.flags|=4),(g.hostBindings!==null||g.hostAttrs!==null||g.hostVars!==0)&&(e.flags|=64);let b=g.type.prototype;!l&&(b.ngOnChanges||b.ngOnInit||b.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),l=!0),!d&&(b.ngOnChanges||b.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),d=!0),f++}$M(t,e,o)}function $M(t,n,e){for(let i=n.directiveStart;i<n.directiveEnd;i++){let r=t.data[i];if(e===null||!e.has(r))Sy(0,n,r,i),Sy(1,n,r,i),Ty(n,i,!1);else{let o=e.get(r);My(0,n,o,i),My(1,n,o,i),Ty(n,i,!0)}}}function Sy(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;t===0?s=n.inputs??={}:s=n.outputs??={},s[o]??=[],s[o].push(i),o0(n,o)}}function My(t,n,e,i){let r=t===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;t===0?a=n.hostDirectiveInputs??={}:a=n.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),o0(n,s)}}function o0(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function Ty(t,n,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=t;if(i===null||!e&&r===null||e&&o===null||Zp(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!e&&r.hasOwnProperty(c)){let l=r[c];for(let d of l)if(d===n){s??=[],s.push(c,i[a+1]);break}}else if(e&&o.hasOwnProperty(c)){let l=o[c];for(let d=0;d<l.length;d+=2)if(l[d]===n){s??=[],s.push(l[d+1],i[a+1]);break}}a+=2}t.initialInputs??=[],t.initialInputs.push(s)}function GM(t,n,e,i,r){t.data[i]=r;let o=r.factory||(r.factory=Vr(r.type,!0)),s=new Jr(o,Jn(r),x,null);t.blueprint[i]=s,e[i]=s,WM(t,n,i,Pb(t,e,r.hostVars,bn),r)}function WM(t,n,e,i,r){let o=r.hostBindings;if(o){let s=t.hostBindingOpCodes;s===null&&(s=t.hostBindingOpCodes=[]);let a=~n.index;qM(s)!=a&&s.push(a),s.push(e,i,o)}}function qM(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function YM(t,n,e){if(e){if(n.exportAs)for(let i=0;i<n.exportAs.length;i++)e[n.exportAs[i]]=t;Jn(n)&&(e[""]=t)}}function ZM(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function gm(t,n,e,i,r,o,s,a){let c=n[P],l=c.consts,d=jn(l,s),f=rs(c,t,e,i,d);return o&&r0(c,n,f,jn(l,a),r),f.mergedAttrs=Jo(f.mergedAttrs,f.attrs),f.attrs!==null&&Dd(f,f.attrs,!1),f.mergedAttrs!==null&&Dd(f,f.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,f),f}function vm(t,n){$y(t,n),Mh(n)&&t.queries.elementEnd(n)}function KM(t,n,e,i,r,o){let s=n.consts,a=jn(s,r),c=rs(n,t,e,i,a);if(c.mergedAttrs=Jo(c.mergedAttrs,c.attrs),o!=null){let l=jn(s,o);c.localNames=[];for(let d=0;d<l.length;d+=2)c.localNames.push(l[d],-1)}return c.attrs!==null&&Dd(c,c.attrs,!1),c.mergedAttrs!==null&&Dd(c,c.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,c),c}var s0=typeof ShadowRoot<"u",QM=typeof Document<"u";function XM(t){return Object.keys(t).map(n=>{let[e,i,r]=t[n],o={propName:e,templateName:n,isSignal:(i&Pd.SignalBased)!==0};return r&&(o.transform=r),o})}function JM(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function eT(t,n,e){let i=n instanceof xe?n:n?.injector;return i&&t.getStandaloneInjector!==null&&(i=t.getStandaloneInjector(i)||i),i?new Cd(e,i):e}function tT(t){let n=t.get(et,null);if(n===null)throw new D(407,!1);let e=t.get(Jb,null),i=t.get(Qn,null),r=t.get(ii,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function nT(t,n){let e=a0(t);return yb(n,e,e==="svg"?Th:e==="math"?M_:null)}function iT(t){if(t?.toLowerCase()==="script")throw new D(905,!1)}function a0(t){return(t.selectors[0][0]||"div").toLowerCase()}var ts=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=XM(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=JM(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=bS(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,i,r,o,s){we(ve.DynamicComponentStart);let a=G(null);try{let c=this.componentDef,l=eT(c,r||this.ngModule,n),d=tT(l),f=d.tracingService;return f&&f.componentCreate?f.componentCreate(e0(c),()=>this.createComponentRef(d,l,e,i,o,s)):this.createComponentRef(d,l,e,i,o,s)}finally{G(a)}}createComponentRef(n,e,i,r,o,s){let a=this.componentDef,c=rT(r,a,s,o),l=n.rendererFactory.createRenderer(null,a),d=r?qS(l,r,a.encapsulation,e):nT(a,l);iT(d?.tagName);let f=e.get(io,null),m=oT(d,()=>e.get(U,null)??cb());f&&f.addHost(m);let g=s?.some(Ay)||o?.some(N=>typeof N!="function"&&N.bindings.some(Ay)),b=rm(null,c,null,512|Fb(a),null,null,n,l,e,null,ub(d,e,!0));f&&s0&&m instanceof ShadowRoot&&Yl(b,()=>{f.removeHost(m)}),b[Ke]=d,Xl(b);let I=null;try{let N=gm(Ke,b,2,"#host",()=>c.directiveRegistry,!0,0);Cb(l,d,N),es(d,b),Ld(c,b,N),Up(c,N,b),vm(c,N),i!==void 0&&aT(N,this.ngContentSelectors,i),I=_n(N.index,b),b[bt]=I[bt],dm(c,b,null)}catch(N){throw I!==null&&fp(I),fp(b),N}finally{we(ve.DynamicComponentEnd),Jl()}return new xd(this.componentType,b,!!g)}};function rT(t,n,e,i){let r=t?["ng-version","22.0.5"]:CS(n.selectors[0]),o=null,s=null,a=0;if(e)for(let d of e)a+=d[Sp].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(i)for(let d=0;d<i.length;d++){let f=i[d];if(typeof f!="function")for(let m of f.bindings){a+=m[Sp].requiredVars;let g=d+1;m.create&&(m.targetIdx=g,(o??=[]).push(m)),m.update&&(m.targetIdx=g,(s??=[]).push(m))}}let c=[n];if(i)for(let d of i){let f=typeof d=="function"?d:d.type,m=_h(f);c.push(m)}return im(0,null,sT(o,s),1,a,c,null,null,null,[r],null)}function oT(t,n){let e=t.getRootNode?.();return QM&&e instanceof Document?e.head:e&&s0&&e instanceof ShadowRoot?e:n().head}function sT(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let i of t)i.create();if(e&2&&n)for(let i of n)i.update()}}function Ay(t){let n=t[Sp].kind;return n==="input"||n==="twoWay"}var xd=class extends Xb{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=Gl(e[P],Ke),this.location=is(this._tNode,e),this.instance=_n(this._tNode.index,e)[bt],this.hostView=this.changeDetectorRef=new Ji(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let r=this._rootLView,o=lm(i,r[P],r,n,e);this.previousInputValues.set(n,e);let s=_n(i.index,r);fm(s,1)}get injector(){return new Xi(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function aT(t,n,e){let i=t.projection=[];for(let r=0;r<n.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var Rt=(()=>{class t{static __NG_ELEMENT_ID__=cT}return t})();function cT(){let t=Qe();return c0(t,Z())}var Mp=class t extends Rt{_lContainer;_hostTNode;_hostLView;constructor(n,e,i){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=i}get element(){return is(this._hostTNode,this._hostLView)}get injector(){return new Xi(this._hostTNode,this._hostLView)}get parentInjector(){let n=Vp(this._hostTNode,this._hostLView);if(qy(n)){let e=gd(n,this._hostLView),i=md(n),r=e[P].data[i+8];return new Xi(r,e)}else return new Xi(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=Ry(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-Ct}createEmbeddedView(n,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=Ip(this._lContainer,n.ssrId),a=n.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,r,yd(this._hostTNode,s)),a}createComponent(n,e,i,r,o,s,a){let c,l=e||{};c=l.index,i=l.injector,r=l.projectableNodes,o=l.environmentInjector||l.ngModuleRef,s=l.directives,a=l.bindings;let d=new ts(zi(n)),f=i||this.parentInjector;if(!o&&d.ngModule==null){let ie=this.parentInjector.get(xe,null);ie&&(o=ie)}let m=zi(d.componentType??{}),g=Ip(this._lContainer,m?.id??null),b=g?.firstChild??null,I=d.create(f,r,b,o,s,a);return this.insertImpl(I.hostView,c,yd(this._hostTNode,g)),I}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,i){let r=n._lView;if(R_(r)){let a=this.indexOf(n);if(a!==-1)this.detach(a);else{let c=r[dt],l=new t(c,c[At],c[dt]);l.detach(l.indexOf(n))}}let o=this._adjustIndex(e),s=this._lContainer;return hm(s,r,o,i),n.attachToViewContainerRef(),Ch(np(s),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=Ry(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),i=bd(this._lContainer,e);i&&(da(np(this._lContainer),e),em(i[P],i))}detach(n){let e=this._adjustIndex(n,-1),i=bd(this._lContainer,e);return i&&da(np(this._lContainer),e)!=null?new Ji(i):null}_adjustIndex(n,e=0){return n??this.length+e}};function Ry(t){return t[ha]}function np(t){return t[ha]||(t[ha]=[])}function c0(t,n){let e,i=n[t.index];return mn(i)?e=i:(e=Kb(i,n,null,t),n[t.index]=e,om(n,e)),dT(e,n,t,i),new Mp(e,t,n)}function lT(t,n){let e=t[Me],i=e.createComment(""),r=vn(n,t),o=e.parentNode(r);return _d(e,o,i,e.nextSibling(r),!1),i}var dT=hT,uT=()=>!1;function fT(t,n,e){return uT(t,n,e)}function hT(t,n,e,i){if(t[Zi])return;let r;e.type&8?r=gn(i):r=lT(n,e),t[Zi]=r}var Tp=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},Ap=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let i=n.contentQueries!==null?n.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new t(r)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)ym(n,e).matches!==null&&this.queries[e].setDirty()}},Id=class{flags;read;predicate;constructor(n,e,i=null){this.flags=e,this.read=i,typeof n=="string"?this.predicate=_T(n):this.predicate=n}},Rp=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(n,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},Np=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=n.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(n,e,pT(e,o)),this.matchTNodeWithReadOption(n,e,fd(e,n,o,!1,!1))}else i===Jt?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,fd(e,n,i,!1,!1))}matchTNodeWithReadOption(n,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===j||r===Rt||r===Jt&&e.type&4)this.addMatch(e.index,-2);else{let o=fd(e,n,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function pT(t,n){let e=t.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===n)return e[i+1]}return null}function mT(t,n){return t.type&11?is(t,n):t.type&4?Vd(t,n):null}function gT(t,n,e,i){return e===-1?mT(n,t):e===-2?vT(t,n,i):wa(t,t[P],e,n)}function vT(t,n,e){if(e===j)return is(n,t);if(e===Jt)return Vd(n,t);if(e===Rt)return c0(n,t)}function l0(t,n,e,i){let r=n[Xn].queries[i];if(r.matches===null){let o=t.data,s=e.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let d=o[l];a.push(gT(n,d,s[c+1],e.metadata.read))}}r.matches=a}return r.matches}function kp(t,n,e,i){let r=t.queries.getByIndex(e),o=r.matches;if(o!==null){let s=l0(t,n,r,e);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)i.push(s[a/2]);else{let l=o[a+1],d=n[-c];for(let f=Ct;f<d.length;f++){let m=d[f];m[qi]===m[dt]&&kp(m[P],m,l,i)}if(d[Gr]!==null){let f=d[Gr];for(let m=0;m<f.length;m++){let g=f[m];kp(g[P],g,l,i)}}}}}return i}function _m(t,n){return t[Xn].queries[n].queryList}function d0(t,n,e){let i=new zn((e&4)===4);return O_(t,n,i,i.destroy),(n[Xn]??=new Ap).queries.push(new Tp(i))-1}function u0(t,n,e){let i=Ae();return i.firstCreatePass&&(h0(i,new Id(t,n,e),-1),(n&2)===2&&(i.staticViewQueries=!0)),d0(i,Z(),n)}function f0(t,n,e,i){let r=Ae();if(r.firstCreatePass){let o=Qe();h0(r,new Id(n,e,i),o.index),yT(r,t),(e&2)===2&&(r.staticContentQueries=!0)}return d0(r,Z(),e)}function _T(t){return t.split(",").map(n=>n.trim())}function h0(t,n,e){t.queries===null&&(t.queries=new Rp),t.queries.track(new Np(n,e))}function yT(t,n){let e=t.contentQueries||(t.contentQueries=[]),i=e.length?e[e.length-1]:-1;n!==i&&e.push(t.queries.length-1,n)}function ym(t,n){return t.queries.getByIndex(n)}function p0(t,n){let e=t[P],i=ym(e,n);return i.crossesNgTemplate?kp(e,t,n,[]):l0(e,t,i,n)}function m0(t,n,e){let i,r=qs(()=>{i._dirtyCounter();let o=bT(i,t);if(n&&o===void 0)throw new D(-951,!1);return o});return i=r[qe],i._dirtyCounter=ee(0),i._flatValue=void 0,r}function bm(t){return m0(!0,!1,t)}function Cm(t){return m0(!0,!0,t)}function g0(t,n){let e=t[qe];e._lView=Z(),e._queryIndex=n,e._queryList=_m(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function bT(t,n){let e=t._lView,i=t._queryIndex;if(e===void 0||i===void 0||e[W]&4)return n?void 0:Tt;let r=_m(e,i),o=p0(e,i);return r.reset(o,nb),n?r.first:r._changesDetected||t._flatValue===void 0?t._flatValue=r.toArray():t._flatValue}function tr(t){return!!t&&typeof t.then=="function"}function Em(t){return!!t&&typeof t.subscribe=="function"}var xi=class{},jd=class{};var Sd=class extends xi{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];constructor(n,e,i,r=!0){super(),this.ngModuleType=n,this._parent=e;let o=f_(n);this._bootstrapComponents=lS(o.bootstrap),this._r3Injector=qh(n,e,[{provide:xi,useValue:this},...i],ca(n),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},Md=class extends jd{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new Sd(this.moduleType,n,[])}};var Ia=class extends xi{injector;instance=null;constructor(n){super();let e=new Br([...n.providers,{provide:xi,useValue:this}],n.parent||Ho(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function Oa(t,n,e=null){return new Ia({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var CT=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=wh(!1,e.type),r=i.length>0?Oa([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=V({token:t,providedIn:"environment",factory:()=>new t(M(xe))})}return t})();function R(t){return Ma(()=>{let n=v0(t),e=H(C({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection!==jp.Eager,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?r=>r.get(CT).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||$n.Emulated,styles:t.styles||Tt,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&er("NgStandalone"),_0(e);let i=t.dependencies;return e.directiveDefs=Ny(i,ET),e.pipeDefs=Ny(i,h_),e.id=xT(e),e})}function ET(t){return zi(t)||_h(t)}function q(t){return Ma(()=>({type:t.type,bootstrap:t.bootstrap||Tt,declarations:t.declarations||Tt,imports:t.imports||Tt,exports:t.exports||Tt,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function wT(t,n){if(t==null)return Gi;let e={};for(let i in t)if(t.hasOwnProperty(i)){let r=t[i],o,s,a,c;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,c=r[3]||null):(o=r,s=r,a=Pd.None,c=null),e[o]=[i,a,c],n[o]=s}return e}function DT(t){if(t==null)return Gi;let n={};for(let e in t)t.hasOwnProperty(e)&&(n[t[e]]=e);return n}function k(t){return Ma(()=>{let n=v0(t);return _0(n),n})}function v0(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||Gi,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||Tt,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:wT(t.inputs,n),outputs:DT(t.outputs),debugInfo:null}}function _0(t){t.features?.forEach(n=>n(t))}function Ny(t,n){return t?()=>{let e=typeof t=="function"?t():t,i=[];for(let r of e){let o=n(r);o!==null&&i.push(o)}return i}:null}function xT(t){let n=0,e=typeof t.consts=="function"?"":t.consts,i=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of i.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}var y0=new y("");var wm=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=u(y0,{optional:!0})??[];injector=u(ae);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=ut(this.injector,r);if(tr(o))e.push(o);else if(Em(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});e.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function Dm(t){return n=>{n.controlDef={create:(e,i)=>{e?.\u0275ngControlCreate(i)},update:(e,i)=>{e?.\u0275ngControlUpdate?.(i)},passThroughInput:t}}}function IT(t){return Object.getPrototypeOf(t.prototype).constructor}function Be(t){let n=IT(t.type),e=!0,i=[t];for(;n;){let r;if(Jn(t))r=n.\u0275cmp||n.\u0275dir;else{if(n.\u0275cmp)throw new D(903,!1);r=n.\u0275dir}if(r){if(e){i.push(r);let s=t;s.inputs=ip(t.inputs),s.declaredInputs=ip(t.declaredInputs),s.outputs=ip(t.outputs);let a=r.hostBindings;a&&RT(t,a);let c=r.viewQuery,l=r.contentQueries;if(c&&TT(t,c),l&&AT(t,l),ST(t,r),u_(t.outputs,r.outputs),Jn(r)&&r.data.animation){let d=t.data;d.animation=(d.animation||[]).concat(r.data.animation)}}let o=r.features;if(o)for(let s=0;s<o.length;s++){let a=o[s];a&&a.ngInherit&&a(t),a===Be&&(e=!1)}}n=Object.getPrototypeOf(n)}MT(i)}function ST(t,n){for(let e in n.inputs){if(!n.inputs.hasOwnProperty(e)||t.inputs.hasOwnProperty(e))continue;let i=n.inputs[e];i!==void 0&&(t.inputs[e]=i,t.declaredInputs[e]=n.declaredInputs[e])}}function MT(t){let n=0,e=null;for(let i=t.length-1;i>=0;i--){let r=t[i];r.hostVars=n+=r.hostVars,r.hostAttrs=Jo(r.hostAttrs,e=Jo(e,r.hostAttrs))}}function ip(t){return t===Gi?{}:t===Tt?[]:t}function TT(t,n){let e=t.viewQuery;e?t.viewQuery=(i,r)=>{n(i,r),e(i,r)}:t.viewQuery=n}function AT(t,n){let e=t.contentQueries;e?t.contentQueries=(i,r,o)=>{n(i,r,o),e(i,r,o)}:t.contentQueries=n}function RT(t,n){let e=t.hostBindings;e?t.hostBindings=(i,r)=>{n(i,r),e(i,r)}:t.hostBindings=n}function b0(t,n,e,i,r,o,s,a){if(e.firstCreatePass){t.mergedAttrs=Jo(t.mergedAttrs,t.attrs);let d=t.tView=im(2,t,r,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),d.queries=e.queries.embeddedTView(t))}a&&(t.flags|=a),Yo(t,!1);let c=kT(e,n,t,i);td()&&tm(e,n,c,t),es(c,n);let l=Kb(c,n,c,t);n[i+Ke]=l,om(n,l),fT(l,t,n)}function NT(t,n,e,i,r,o,s,a,c,l,d){let f=e+Ke,m;return n.firstCreatePass?(m=rs(n,f,4,s||null,a||null),Zl()&&r0(n,t,m,jn(n.consts,l),sm),$y(n,m)):m=n.data[f],b0(m,t,n,e,i,r,o,c),Wo(m)&&Ld(n,t,m),l!=null&&Na(t,m,d),m}function Bd(t,n,e,i,r,o,s,a,c,l,d){let f=e+Ke,m;if(n.firstCreatePass){if(m=rs(n,f,4,s||null,a||null),l!=null){let g=jn(n.consts,l);m.localNames=[];for(let b=0;b<g.length;b+=2)m.localNames.push(g[b],-1)}}else m=n.data[f];return b0(m,t,n,e,i,r,o,c),l!=null&&Na(t,m,d),m}function $(t,n,e,i,r,o,s,a){let c=Z(),l=Ae(),d=jn(l.consts,o);return NT(c,l,t,n,e,i,r,d,void 0,s,a),$}function Ud(t,n,e,i,r,o,s,a){let c=Z(),l=Ae(),d=jn(l.consts,o);return Bd(c,l,t,n,e,i,r,d,void 0,s,a),Ud}var kT=OT;function OT(t,n,e,i){return ga(!0),n[Me].createComment("")}var Hd=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();var xm=new y("");var Fa=new y("");function C0(){kf(()=>{let t="";throw new D(600,t)})}var FT=10;var en=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=u(sn);afterRenderManager=u(kd);zonelessEnabled=u(_a);rootEffectScheduler=u(rd);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new S;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=u(wi);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(ne(e=>!e))}constructor(){u(ii,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=u(xe);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=ae.NULL){return this._injector.get(O).run(()=>{if(we(ve.BootstrapComponentStart),!this._injector.get(wm).done){let ie="";throw new D(405,ie)}let a=zi(e),c=this._injector.get(xi),l=new ts(a,c);this.componentTypes.push(e);let{hostElement:d,directives:f,bindings:m}=PT(i),g=d||l.selector,b=l.create(r,[],g,c.injector,f,m),I=b.location.nativeElement,N=b.injector.get(xm,null);return N?.registerApplication(I),b.onDestroy(()=>{this.detachView(b.hostView),Ea(this.components,b),N?.unregisterApplication(I)}),this._loadComponent(b),we(ve.BootstrapComponentEnd,b),b})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){we(ve.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Nd.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw we(ve.ChangeDetectionEnd),new D(101,!1);let e=G(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,G(e),this.afterTick.next(),we(ve.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(et,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<FT;){we(ve.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{we(ve.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!pa(r))continue;let o=i&&!this.zonelessEnabled?0:1;Wb(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>pa(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;Ea(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Fa,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>Ea(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new D(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function PT(t){return t===void 0||typeof t=="string"||t instanceof Element?{hostElement:t}:t}function Ea(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function he(t,n,e,i){let r=Z(),o=Yr();if(Mi(r,o,n)){let s=Ae(),a=Zo();tM(a,r,t,n,e,i)}return he}function _e(t,n,e,i,r,o,s,a){er("NgControlFlow");let c=Z(),l=Ae(),d=jn(l.consts,o);return Bd(c,l,t,n,e,i,r,d,256,s,a),Im}function Im(t,n,e,i,r,o,s,a){er("NgControlFlow");let c=Z(),l=Ae(),d=jn(l.consts,o);return Bd(c,l,t,n,e,i,r,d,512,s,a),Im}function ye(t,n){er("NgControlFlow");let e=Z(),i=Yr(),r=e[i]!==bn?e[i]:-1,o=r!==-1?ky(e,Ke+r):void 0,s=0;if(Mi(e,i,t)){let a=G(null);try{if(o!==void 0&&EM(o,s),t!==-1){let c=Ke+t,l=ky(e,c),d=LT(e[P],c),f=AM(l,d,e),m=um(e,d,n,{dehydratedView:f});hm(l,m,s,yd(d,f))}}finally{G(a)}}else if(o!==void 0){let a=CM(o,s);a!==void 0&&(a[bt]=n)}}function ky(t,n){return t[n]}function LT(t,n){return Gl(t,n)}function E(t,n,e){let i=Z(),r=Yr();if(Mi(i,r,n)){let o=Ae(),s=Zo();jb(s,i,t,n,i[Me],e)}return E}function Op(t,n,e,i,r){lm(n,t,e,r?"class":"style",i)}function p(t,n,e,i){let r=Z(),o=r[P],s=t+Ke,a=o.firstCreatePass?gm(s,r,2,n,sm,Zl(),e,i):o.data[s];if(Ei(a)){let c=r[Vn].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(e0(l),()=>(Oy(t,n,r,a,i),p))}}return Oy(t,n,r,a,i),p}function Oy(t,n,e,i,r){if(am(i,e,t,n,E0),Wo(i)){let o=e[P];Ld(o,e,i),Up(o,i,e)}r!=null&&Na(e,i)}function h(){let t=Ae(),n=Qe(),e=cm(n);return t.firstCreatePass&&vm(t,e),Lh(e)&&Vh(),Fh(),e.classesWithoutHost!=null&&uI(e)&&Op(t,e,Z(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&fI(e)&&Op(t,e,Z(),e.stylesWithoutHost,!1),h}function pe(t,n,e,i){return p(t,n,e,i),h(),pe}function ft(t,n,e,i){let r=Z(),o=r[P],s=t+Ke,a=o.firstCreatePass?KM(s,o,2,n,e,i):o.data[s];return am(a,r,t,n,E0),i!=null&&Na(r,a),ft}function wt(){let t=Qe(),n=cm(t);return Lh(n)&&Vh(),Fh(),wt}function En(t,n,e,i){return ft(t,n,e,i),wt(),En}var E0=(t,n,e,i,r)=>(ga(!0),yb(n[Me],i,Wh()));function Sm(t,n,e){let i=Z(),r=i[P],o=t+Ke,s=r.firstCreatePass?gm(o,i,8,"ng-container",sm,Zl(),n,e):r.data[o];if(am(s,i,t,"ng-container",VT),Wo(s)){let a=i[P];Ld(a,i,s),Up(a,s,i)}return e!=null&&Na(i,s),Sm}function Mm(){let t=Ae(),n=Qe(),e=cm(n);return t.firstCreatePass&&vm(t,e),Mm}function os(t,n,e){return Sm(t,n,e),Mm(),os}var VT=(t,n,e,i,r)=>(ga(!0),iS(n[Me],""));function Ue(){return Z()}function wn(t,n,e){let i=Z(),r=Yr();if(Mi(i,r,n)){let o=Ae(),s=Zo();Bb(s,i,t,n,i[Me],e)}return wn}var Pa="en-US";var jT=Pa;function w0(t){typeof t=="string"&&(jT=t.toLowerCase().replace(/_/g,"-"))}function F(t,n,e){let i=Z(),r=Ae(),o=Qe();return D0(r,i,i[Me],o,t,n,e),F}function zd(t,n,e){let i=Z(),r=Ae(),o=Qe();return(o.type&3||e)&&mm(o,r,i,e,i[Me],t,n,Xr(o,i,n)),zd}function D0(t,n,e,i,r,o,s){let a=!0,c=null;if((i.type&3||s)&&(c??=Xr(i,n,o),mm(i,t,n,s,e,r,o,c)&&(a=!1)),a){let l=i.outputs?.[r],d=i.hostDirectiveOutputs?.[r];if(d&&d.length)for(let f=0;f<d.length;f+=2){let m=d[f],g=d[f+1];c??=Xr(i,n,o),Ed(i,n,m,g,r,c)}if(l&&l.length)for(let f of l)c??=Xr(i,n,o),Ed(i,n,f,r,r,c)}}function w(t=1){return Y_(t)}function BT(t,n){let e=null,i=mS(t);for(let r=0;r<n.length;r++){let o=n[r];if(o==="*"){e=r;continue}if(i===null?Ib(t,o,!0):_S(i,o))return r}return e}function He(t){let n=Z()[Xt][At];if(!n.projection){let e=t?t.length:1,i=n.projection=y_(e,null),r=i.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let s=t?BT(o,t):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function ue(t,n=0,e,i,r,o){let s=Z(),a=Ae(),c=i?t+1:null;c!==null&&Bd(s,a,c,i,r,o,null,e);let l=rs(a,Ke+t,16,null,e||null);l.projection===null&&(l.projection=n),Uh();let f=!s[zo]||Ph();s[Xt][At].projection[l.projection]===null&&c!==null?UT(s,a,c):f&&!Ad(l)&&US(a,s,l)}function UT(t,n,e){let i=Ke+e,r=n.data[i],o=t[i],s=Ip(o,r.tView.ssrId),a=um(t,r,void 0,{dehydratedView:s});hm(o,a,0,yd(r,s))}function Dn(t,n,e,i){return f0(t,n,e,i),Dn}function Nt(t,n,e){return u0(t,n,e),Nt}function K(t){let n=Z(),e=Ae(),i=Ql();ma(i+1);let r=ym(e,i);if(t.dirty&&A_(n)===((r.metadata.flags&2)===2)){if(r.matches===null)t.reset([]);else{let o=p0(n,i);t.reset(o,nb),t.notifyOnChanges()}return!0}return!1}function Q(){return _m(Z(),Ql())}function $d(t,n,e,i,r){return g0(n,f0(t,e,i,r)),$d}function Gd(t,n,e,i){return g0(t,u0(n,e,i)),Gd}function Wd(t=1){ma(Ql()+t)}function ri(t){let n=V_();return T_(n,Ke+t)}function ld(t,n){return t<<17|n<<2}function no(t){return t>>17&32767}function HT(t){return(t&2)==2}function zT(t,n){return t&131071|n<<17}function Fp(t){return t|2}function ns(t){return(t&131068)>>2}function rp(t,n){return t&-131069|n<<2}function $T(t){return(t&1)===1}function Pp(t){return t|1}function GT(t,n,e,i,r,o){let s=o?n.classBindings:n.styleBindings,a=no(s),c=ns(s);t[i]=e;let l=!1,d;if(Array.isArray(e)){let f=e;d=f[1],(d===null||Uo(f,d)>0)&&(l=!0)}else d=e;if(r)if(c!==0){let m=no(t[a+1]);t[i+1]=ld(m,a),m!==0&&(t[m+1]=rp(t[m+1],i)),t[a+1]=zT(t[a+1],i)}else t[i+1]=ld(a,0),a!==0&&(t[a+1]=rp(t[a+1],i)),a=i;else t[i+1]=ld(c,0),a===0?a=i:t[c+1]=rp(t[c+1],i),c=i;l&&(t[i+1]=Fp(t[i+1])),Fy(t,d,i,!0),Fy(t,d,i,!1),WT(n,d,t,i,o),s=ld(a,c),o?n.classBindings=s:n.styleBindings=s}function WT(t,n,e,i,r){let o=r?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&Uo(o,n)>=0&&(e[i+1]=Pp(e[i+1]))}function Fy(t,n,e,i){let r=t[e+1],o=n===null,s=i?no(r):ns(r),a=!1;for(;s!==0&&(a===!1||o);){let c=t[s],l=t[s+1];qT(c,n)&&(a=!0,t[s+1]=i?Pp(l):Fp(l)),s=i?no(l):ns(l)}a&&(t[e+1]=i?Fp(r):Pp(r))}function qT(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?Uo(t,n)>=0:!1}var Hn={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function YT(t){return t.substring(Hn.key,Hn.keyEnd)}function ZT(t){return KT(t),x0(t,I0(t,0,Hn.textEnd))}function x0(t,n){let e=Hn.textEnd;return e===n?-1:(n=Hn.keyEnd=QT(t,Hn.key=n,e),I0(t,n,e))}function KT(t){Hn.key=0,Hn.keyEnd=0,Hn.value=0,Hn.valueEnd=0,Hn.textEnd=t.length}function I0(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function QT(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function ro(t,n,e){return S0(t,n,e,!1),ro}function ce(t,n){return S0(t,n,null,!0),ce}function an(t){JT(oA,XT,t,!0)}function XT(t,n){for(let e=ZT(n);e>=0;e=x0(n,e))Hl(t,YT(n),!0)}function S0(t,n,e,i){let r=Z(),o=Ae(),s=zh(2);if(o.firstUpdatePass&&T0(o,t,s,i),n!==bn&&Mi(r,s,n)){let a=o.data[Ki()];A0(o,a,r,r[Me],t,r[s+1]=aA(n,e),i,s)}}function JT(t,n,e,i){let r=Ae(),o=zh(2);r.firstUpdatePass&&T0(r,null,o,i);let s=Z();if(e!==bn&&Mi(s,o,e)){let a=r.data[Ki()];if(R0(a,i)&&!M0(r,o)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(e=Pl(c,e||"")),Op(r,a,s,e,i)}else sA(r,a,s,s[Me],s[o+1],s[o+1]=rA(t,n,e),i,o)}}function M0(t,n){return n>=t.expandoStartIndex}function T0(t,n,e,i){let r=t.data;if(r[e+1]===null){let o=r[Ki()],s=M0(t,e);R0(o,i)&&n===null&&!s&&(n=!1),n=eA(r,o,n,i),GT(r,o,n,e,s,i)}}function eA(t,n,e,i){let r=$_(t),o=i?n.residualClasses:n.residualStyles;if(r===null)(i?n.classBindings:n.styleBindings)===0&&(e=op(null,t,n,e,i),e=Sa(e,n.attrs,i),o=null);else{let s=n.directiveStylingLast;if(s===-1||t[s]!==r)if(e=op(r,t,n,e,i),o===null){let c=tA(t,n,i);c!==void 0&&Array.isArray(c)&&(c=op(null,t,n,c[1],i),c=Sa(c,n.attrs,i),nA(t,n,i,c))}else o=iA(t,n,i)}return o!==void 0&&(i?n.residualClasses=o:n.residualStyles=o),e}function tA(t,n,e){let i=e?n.classBindings:n.styleBindings;if(ns(i)!==0)return t[no(i)]}function nA(t,n,e,i){let r=e?n.classBindings:n.styleBindings;t[no(r)]=i}function iA(t,n,e){let i,r=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<r;o++){let s=t[o].hostAttrs;i=Sa(i,s,e)}return Sa(i,n.attrs,e)}function op(t,n,e,i,r){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=n[a],i=Sa(i,o.hostAttrs,r),o!==t);)a++;return t!==null&&(e.directiveStylingLast=a),i}function Sa(t,n,e){let i=e?1:2,r=-1;if(n!==null)for(let o=0;o<n.length;o++){let s=n[o];typeof s=="number"?r=s:r===i&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),Hl(t,s,e?!0:n[++o]))}return t===void 0?null:t}function rA(t,n,e){if(e==null||e==="")return Tt;let i=[],r=Wn(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)t(i,r[o],!0);else if(r instanceof Set)for(let o of r)t(i,o,!0);else if(typeof r=="object")for(let o in r)Object.hasOwn(r,o)&&t(i,o,r[o]);else typeof r=="string"&&n(i,r);return i}function oA(t,n,e){let i=String(n);i!==""&&!i.includes(" ")&&Hl(t,i,e)}function sA(t,n,e,i,r,o,s,a){r===bn&&(r=Tt);let c=0,l=0,d=0<r.length?r[0]:null,f=0<o.length?o[0]:null;for(;d!==null||f!==null;){let m=c<r.length?r[c+1]:void 0,g=l<o.length?o[l+1]:void 0,b=null,I;d===f?(c+=2,l+=2,m!==g&&(b=f,I=g)):f===null||d!==null&&d<f?(c+=2,b=d):(l+=2,b=f,I=g),b!==null&&A0(t,n,e,i,b,I,s,a),d=c<r.length?r[c]:null,f=l<o.length?o[l]:null}}function A0(t,n,e,i,r,o,s,a){if(!(n.type&3))return;let c=t.data,l=c[a+1],d=$T(l)?Py(c,n,e,r,ns(l),s):void 0;if(!Td(d)){Td(o)||HT(l)&&(o=Py(c,null,e,r,a,s));let f=Ah(Ki(),e);zS(i,s,f,r,o)}}function Py(t,n,e,i,r,o){let s=n===null,a;for(;r>0;){let c=t[r],l=Array.isArray(c),d=l?c[1]:c,f=d===null,m=e[r+1];m===bn&&(m=f?Tt:void 0);let g=f?zl(m,i):d===i?m:void 0;if(l&&!Td(g)&&(g=zl(c,i)),Td(g)&&(a=g,s))return a;let b=t[r+1];r=s?no(b):ns(b)}if(n!==null){let c=o?n.residualClasses:n.residualStyles;c!=null&&(a=zl(c,i))}return a}function Td(t){return t!==void 0}function aA(t,n){return t==null||t===""||(typeof n=="string"?t=t+n:typeof t=="object"&&(t=ca(Wn(t)))),t}function R0(t,n){return(t.flags&(n?8:16))!==0}function _(t,n=""){let e=Z(),i=Ae(),r=t+Ke,o=i.firstCreatePass?rs(i,r,1,n,null):i.data[r],s=cA(i,e,o,n);e[r]=s,td()&&tm(i,e,s,o),Yo(o,!1)}var cA=(t,n,e,i)=>(ga(!0),tS(n[Me],i));function lA(t,n,e,i=""){return Mi(t,Yr(),e)?n+jl(e)+i:bn}function nt(t){return be("",t),nt}function be(t,n,e){let i=Z(),r=lA(i,t,n,e);return r!==bn&&dA(i,Ki(),r),be}function dA(t,n,e){let i=Ah(n,t);nS(t[Me],i,e)}function Re(t,n,e){od(n)&&(n=n());let i=Z(),r=Yr();if(Mi(i,r,n)){let o=Ae(),s=Zo();jb(s,i,t,n,i[Me],e)}return Re}function Pe(t,n){let e=od(t);return e&&t.set(n),e}function Ne(t,n){let e=Z(),i=Ae(),r=Qe();return D0(i,e,e[Me],r,t,n),Ne}function Ly(t,n,e){let i=Ae();i.firstCreatePass&&N0(n,i.data,i.blueprint,Jn(t),e)}function N0(t,n,e,i,r){if(t=yt(t),Array.isArray(t))for(let o=0;o<t.length;o++)N0(t[o],n,e,i,r);else{let o=Ae(),s=Z(),a=Qe(),c=jr(t)?t:yt(t.provide),l=xh(t),d=a.providerIndexes&1048575,f=a.directiveStart,m=a.providerIndexes>>20;if(jr(t)||!t.multi){let g=new Jr(l,r,x,null),b=ap(c,n,r?d:d+m,f);b===-1?(dp(vd(a,s),o,c),sp(o,t,n.length),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(g),s.push(g)):(e[b]=g,s[b]=g)}else{let g=ap(c,n,d+m,f),b=ap(c,n,d,d+m),I=g>=0&&e[g],N=b>=0&&e[b];if(r&&!N||!r&&!I){dp(vd(a,s),o,c);let ie=hA(r?fA:uA,e.length,r,i,l,t);!r&&N&&(e[b].providerFactory=ie),sp(o,t,n.length,0),n.push(c),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(ie),s.push(ie)}else{let ie=k0(e[r?b:g],l,!r&&i);sp(o,t,g>-1?g:b,ie)}!r&&i&&N&&e[b].componentProviders++}}}function sp(t,n,e,i){let r=jr(n),o=x_(n);if(r||o){let c=(o?yt(n.useClass):n).prototype.ngOnDestroy;if(c){let l=t.destroyHooks||(t.destroyHooks=[]);if(!r&&n.multi){let d=l.indexOf(e);d===-1?l.push(e,[i,c]):l[d+1].push(i,c)}else l.push(e,c)}}}function k0(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function ap(t,n,e,i){for(let r=e;r<i;r++)if(n[r]===t)return r;return-1}function uA(t,n,e,i,r){return Lp(this.multi,[])}function fA(t,n,e,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,c=wa(i,i[P],this.providerFactory.index,r);s=c.slice(0,a),Lp(o,s);for(let l=a;l<c.length;l++)s.push(c[l])}else s=[],Lp(o,s);return s}function Lp(t,n){for(let e=0;e<t.length;e++){let i=t[e];n.push(i())}return n}function hA(t,n,e,i,r,o){let s=new Jr(t,e,x,null);return s.multi=[],s.index=n,s.componentProviders=0,k0(s,r,i&&!e),s}function ze(t,n){return e=>{e.providersResolver=(i,r)=>Ly(i,r?r(t):t,!1),n&&(e.viewProvidersResolver=(i,r)=>Ly(i,r?r(n):n,!0))}}function oo(t,n,e){return mA(Z(),j_(),t,n,e)}function pA(t,n){let e=t[n];return e===bn?void 0:e}function mA(t,n,e,i,r,o){let s=n+e;return Mi(t,s,r)?NM(t,s+1,o?i.call(o,r):i(r)):pA(t,s+1)}function La(t,n){return Vd(t,n)}var O0=(()=>{class t{applicationErrorHandler=u(sn);appRef=u(en);taskService=u(wi);ngZone=u(O);zonelessEnabled=u(_a);tracing=u(ii,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new fe;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(sa):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(u(Xh,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?X_:Yh;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(sa+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function F0(){return[{provide:Qn,useExisting:O0},{provide:O,useClass:aa},{provide:_a,useValue:!0}]}var Tm=(()=>{class t{compileModuleSync(e){return new Md(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function gA(){return typeof $localize<"u"&&$localize.locale||Pa}var qd=new y("",{factory:()=>u(qd,{optional:!0,skipSelf:!0})||gA()});function qt(t,n){return qs(t,n?.equal)}function ge(t){return Nv(t)}var vA=t=>t;function Am(t,n){if(typeof t=="function"){let e=Vf(t,vA,n?.equal);return P0(e,n?.debugName)}else{let e=Vf(t.source,t.computation,t.equal);return P0(e,t.debugName)}}function P0(t,n){let e=t[qe],i=t;return i.set=r=>Av(e,r),i.update=r=>Rv(e,r),i.asReadonly=nd.bind(t),i}var $0=Symbol("InputSignalNode#UNSET"),FA=H(C({},Ys),{transformFn:void 0,applyValueToInputSignal(t,n){Sr(t,n)}});function G0(t,n){let e=Object.create(FA);e.value=t,e.transformFn=n?.transform;function i(){if(ji(e),e.value===$0){let r=null;throw new D(-950,r)}return e.value}return i[qe]=e,i}var nr=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>Ta(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}},W0=(()=>{let t=new y("");return t.__NG_ELEMENT_ID__=n=>{let e=Qe();if(e===null)throw new D(-204,!1);if(e.type&2)return e.value;if(n&8)return null;throw new D(-204,!1)},t})();function Vm(t){return PA(t)?t.default:t}function PA(t){return t&&typeof t=="object"&&"default"in t}function L0(t,n){return G0(t,n)}function LA(t){return G0($0,t)}var ss=(L0.required=LA,L0);function V0(t,n){return bm(n)}function VA(t,n){return Cm(n)}var ja=(V0.required=VA,V0);function j0(t,n){return bm(n)}function jA(t,n){return Cm(n)}var q0=(j0.required=jA,j0);var BA=1e4;var mW=BA-1e3;var Nm=class{supports(n){return pm(n)}create(n){return new km(n)}},UA=(t,n)=>n,km=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(n){this._trackByFn=n||UA}forEachItem(n){let e;for(e=this._itHead;e!==null;e=e._next)n(e)}forEachOperation(n){let e=this._itHead,i=this._removalsHead,r=0,o=null;for(;e||i;){let s=!i||e&&e.currentIndex<B0(i,r,o)?e:i,a=B0(s,r,o),c=s.currentIndex;if(s===i)r--,i=i._nextRemoved;else if(e=e._next,s.previousIndex==null)r++;else{o||(o=[]);let l=a-r,d=c-r;if(l!=d){for(let m=0;m<l;m++){let g=m<o.length?o[m]:o[m]=0,b=g+m;d<=b&&b<l&&(o[m]=g+1)}let f=s.previousIndex;o[f]=d-l}}a!==c&&n(s,a,c)}}forEachPreviousItem(n){let e;for(e=this._previousItHead;e!==null;e=e._nextPrevious)n(e)}forEachAddedItem(n){let e;for(e=this._additionsHead;e!==null;e=e._nextAdded)n(e)}forEachMovedItem(n){let e;for(e=this._movesHead;e!==null;e=e._nextMoved)n(e)}forEachRemovedItem(n){let e;for(e=this._removalsHead;e!==null;e=e._nextRemoved)n(e)}forEachIdentityChange(n){let e;for(e=this._identityChangesHead;e!==null;e=e._nextIdentityChange)n(e)}diff(n){if(n==null&&(n=[]),!pm(n))throw new D(900,!1);return this.check(n)?this:null}onDestroy(){}check(n){this._reset();let e=this._itHead,i=!1,r,o,s;if(Array.isArray(n)){this.length=n.length;for(let a=0;a<this.length;a++)o=n[a],s=this._trackByFn(a,o),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,o,s,a),i=!0):(i&&(e=this._verifyReinsertion(e,o,s,a)),Object.is(e.item,o)||this._addIdentityChange(e,o)),e=e._next}else r=0,t0(n,a=>{s=this._trackByFn(r,a),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,a,s,r),i=!0):(i&&(e=this._verifyReinsertion(e,a,s,r)),Object.is(e.item,a)||this._addIdentityChange(e,a)),e=e._next,r++}),this.length=r;return this._truncate(e),this.collection=n,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let n;for(n=this._previousItHead=this._itHead;n!==null;n=n._next)n._nextPrevious=n._next;for(n=this._additionsHead;n!==null;n=n._nextAdded)n.previousIndex=n.currentIndex;for(this._additionsHead=this._additionsTail=null,n=this._movesHead;n!==null;n=n._nextMoved)n.previousIndex=n.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(n,e,i,r){let o;return n===null?o=this._itTail:(o=n._prev,this._remove(n)),n=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._reinsertAfter(n,o,r)):(n=this._linkedRecords===null?null:this._linkedRecords.get(i,r),n!==null?(Object.is(n.item,e)||this._addIdentityChange(n,e),this._moveAfter(n,o,r)):n=this._addAfter(new Om(e,i),o,r)),n}_verifyReinsertion(n,e,i,r){let o=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return o!==null?n=this._reinsertAfter(o,n._prev,r):n.currentIndex!=r&&(n.currentIndex=r,this._addToMoves(n,r)),n}_truncate(n){for(;n!==null;){let e=n._next;this._addToRemovals(this._unlink(n)),n=e}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(n,e,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(n);let r=n._prevRemoved,o=n._nextRemoved;return r===null?this._removalsHead=o:r._nextRemoved=o,o===null?this._removalsTail=r:o._prevRemoved=r,this._insertAfter(n,e,i),this._addToMoves(n,i),n}_moveAfter(n,e,i){return this._unlink(n),this._insertAfter(n,e,i),this._addToMoves(n,i),n}_addAfter(n,e,i){return this._insertAfter(n,e,i),this._additionsTail===null?this._additionsTail=this._additionsHead=n:this._additionsTail=this._additionsTail._nextAdded=n,n}_insertAfter(n,e,i){let r=e===null?this._itHead:e._next;return n._next=r,n._prev=e,r===null?this._itTail=n:r._prev=n,e===null?this._itHead=n:e._next=n,this._linkedRecords===null&&(this._linkedRecords=new Zd),this._linkedRecords.put(n),n.currentIndex=i,n}_remove(n){return this._addToRemovals(this._unlink(n))}_unlink(n){this._linkedRecords!==null&&this._linkedRecords.remove(n);let e=n._prev,i=n._next;return e===null?this._itHead=i:e._next=i,i===null?this._itTail=e:i._prev=e,n}_addToMoves(n,e){return n.previousIndex===e||(this._movesTail===null?this._movesTail=this._movesHead=n:this._movesTail=this._movesTail._nextMoved=n),n}_addToRemovals(n){return this._unlinkedRecords===null&&(this._unlinkedRecords=new Zd),this._unlinkedRecords.put(n),n.currentIndex=null,n._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=n,n._prevRemoved=null):(n._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=n),n}_addIdentityChange(n,e){return n.item=e,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=n:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=n,n}},Om=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(n,e){this.item=n,this.trackById=e}},Fm=class{_head=null;_tail=null;add(n){this._head===null?(this._head=this._tail=n,n._nextDup=null,n._prevDup=null):(this._tail._nextDup=n,n._prevDup=this._tail,n._nextDup=null,this._tail=n)}get(n,e){let i;for(i=this._head;i!==null;i=i._nextDup)if((e===null||e<=i.currentIndex)&&Object.is(i.trackById,n))return i;return null}remove(n){let e=n._prevDup,i=n._nextDup;return e===null?this._head=i:e._nextDup=i,i===null?this._tail=e:i._prevDup=e,this._head===null}},Zd=class{map=new Map;put(n){let e=n.trackById,i=this.map.get(e);i||(i=new Fm,this.map.set(e,i)),i.add(n)}get(n,e){let i=n,r=this.map.get(i);return r?r.get(n,e):null}remove(n){let e=n.trackById;return this.map.get(e).remove(n)&&this.map.delete(e),n}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function B0(t,n,e){let i=t.previousIndex;if(i===null)return i;let r=0;return e&&i<e.length&&(r=e[i]),i+n+r}function U0(){return new Kd([new Nm])}var Kd=(()=>{class t{factories;static \u0275prov=V({token:t,providedIn:"root",factory:U0});constructor(e){this.factories=e}static create(e,i){if(i!=null){let r=i.factories.slice();e=e.concat(r)}return new t(e)}static extend(e){return{provide:t,useFactory:()=>{let i=u(t,{optional:!0,skipSelf:!0});return t.create(e,i||U0())}}}find(e){let i=this.factories.find(r=>r.supports(e));if(i!=null)return i;throw new D(901,!1)}}return t})();var me=(()=>{class t{static __NG_ELEMENT_ID__=HA}return t})();function HA(t){return zA(Qe(),Z(),(t&16)===16)}function zA(t,n,e){if(Ei(t)&&!e){let i=_n(t.index,n);return new Ji(i,i)}else if(t.type&175){let i=n[Xt];return new Ji(i,n)}return null}var Pm=new y(""),$A=new y("");function Va(t){return!t.moduleRef}function GA(t){let n=Va(t)?t.r3Injector:t.moduleRef.injector,e=n.get(O);return e.run(()=>{Va(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let i=n.get(sn),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),Va(t)){let o=()=>n.destroy(),s=t.platformInjector.get(Pm);s.add(o),n.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>t.moduleRef.destroy(),s=t.platformInjector.get(Pm);s.add(o),t.moduleRef.onDestroy(()=>{Ea(t.allPlatformModules,t.moduleRef),r.unsubscribe(),s.delete(o)})}return qA(i,e,()=>{let o=n.get(wi),s=o.add(),a=n.get(wm);return a.runInitializers(),a.donePromise.then(()=>{let c=n.get(qd,Pa);if(w0(c||Pa),!n.get($A,!0))return Va(t)?n.get(en):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(Va(t)){let d=n.get(en);return t.rootComponent!==void 0&&d.bootstrap(t.rootComponent),d}else return WA?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(s)})})})}var WA;function qA(t,n,e){try{let i=e();return tr(i)?i.catch(r=>{throw n.runOutsideAngular(()=>t(r)),r}):i}catch(i){throw n.runOutsideAngular(()=>t(i)),i}}var Yd=null;function YA(t=[],n){return ae.create({name:n,providers:[{provide:fa,useValue:"platform"},{provide:Pm,useValue:new Set([()=>Yd=null])},...t]})}function ZA(t=[]){if(Yd)return Yd;let n=YA(t);return Yd=n,C0(),KA(n),n}function KA(t){let n=t.get(id,null);ut(t,()=>{n?.forEach(e=>e())})}function Y0(t){let{rootComponent:n,appProviders:e,platformProviders:i,platformRef:r}=t;we(ve.BootstrapApplicationStart);try{let o=r?.injector??ZA(i),s=[F0(),ey,...e||[]],a=new Ia({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return GA({r3Injector:a.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{we(ve.BootstrapApplicationEnd)}}function B(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function xn(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var Rm=Symbol("NOT_SET"),Z0=new Set,QA=H(C({},Ys),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Rm,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Rm&&!To(this))return this.signal;try{for(let r of this.cleanup??Z0)r()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=fi(this),i;try{i=this.userFn.apply(null,n)}finally{Bi(this,e)}return(this.value===Rm||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),Lm=class extends Da{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,i,r,o,s=null){super(n,[void 0,void 0,void 0,void 0],i,!1,o.get(Et),s),this.scheduler=r;for(let a of Qp){let c=e[a];if(c===void 0)continue;let l=Object.create(QA);l.sequence=this,l.phase=a,l.userFn=c,l.dirty=!0,l.signal=()=>(ji(l),l.value),l.signal[qe]=l,l.registerCleanupFn=d=>(l.cleanup??=new Set).add(d),this.nodes[a]=l,this.hooks[a]=d=>l.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??Z0)e()}finally{Ui(n)}}};function K0(t,n){let e=n?.injector??u(ae),i=e.get(Qn),r=e.get(kd),o=e.get(ii,null,{optional:!0});r.impl??=e.get(Xp);let s=t;typeof s=="function"&&(s={mixedReadWrite:t});let a=e.get(Ko,null,{optional:!0}),c=new Lm(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,e,o?.snapshot(null));return r.impl.register(c),c}function Qd(t,n){let e=zi(t),i=n.elementInjector||Ho();return new ts(e).create(i,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings)}var Q0=null;function In(){return Q0}function jm(t){Q0??=t}var Ba=class{},as=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:()=>u(X0),providedIn:"platform"})}return t})();var X0=(()=>{class t extends as{_location;_history;_doc=u(U);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return In().getBaseHref(this._doc)}onPopState(e){let i=In().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=In().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function tC(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function J0(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function ir(t){return t&&t[0]!=="?"?`?${t}`:t}var cs=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:()=>u(JA),providedIn:"root"})}return t})(),XA=new y(""),JA=(()=>{class t extends cs{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??u(U).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return tC(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+ir(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let s=this.prepareExternalUrl(r+ir(o));this._platformLocation.pushState(e,i,s)}replaceState(e,i,r,o){let s=this.prepareExternalUrl(r+ir(o));this._platformLocation.replaceState(e,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||t)(M(as),M(XA,8))};static \u0275prov=V({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var rr=(()=>{class t{_subject=new S;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=nR(J0(eC(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+ir(i))}normalize(e){return t.stripTrailingSlash(tR(this._basePath,eC(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+ir(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+ir(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=ir;static joinWithSlash=tC;static stripTrailingSlash=J0;static \u0275fac=function(i){return new(i||t)(M(cs))};static \u0275prov=V({token:t,factory:()=>eR(),providedIn:"root"})}return t})();function eR(){return new rr(M(cs))}function tR(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function eC(t){return t.replace(/\/index\.html$/,"")}function nR(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var Xd=class{$implicit;ngForOf;index;count;constructor(n,e,i,r){this.$implicit=n,this.ngForOf=e,this.index=i,this.count=r}get first(){return this.index===0}get last(){return this.index===this.count-1}get even(){return this.index%2===0}get odd(){return!this.even}},tn=(()=>{class t{_viewContainer;_template;_differs;set ngForOf(e){this._ngForOf=e,this._ngForOfDirty=!0}set ngForTrackBy(e){this._trackByFn=e}get ngForTrackBy(){return this._trackByFn}_ngForOf=null;_ngForOfDirty=!0;_differ=null;_trackByFn;constructor(e,i,r){this._viewContainer=e,this._template=i,this._differs=r}set ngForTemplate(e){e&&(this._template=e)}ngDoCheck(){if(this._ngForOfDirty){this._ngForOfDirty=!1;let e=this._ngForOf;!this._differ&&e&&(this._differ=this._differs.find(e).create(this.ngForTrackBy))}if(this._differ){let e=this._differ.diff(this._ngForOf);e&&this._applyChanges(e)}}_applyChanges(e){let i=this._viewContainer;e.forEachOperation((r,o,s)=>{if(r.previousIndex==null)i.createEmbeddedView(this._template,new Xd(r.item,this._ngForOf,-1,-1),s===null?void 0:s);else if(s==null)i.remove(o===null?void 0:o);else if(o!==null){let a=i.get(o);i.move(a,s),nC(a,r)}});for(let r=0,o=i.length;r<o;r++){let a=i.get(r).context;a.index=r,a.count=o,a.ngForOf=this._ngForOf}e.forEachIdentityChange(r=>{let o=i.get(r.currentIndex);nC(o,r)})}static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||t)(x(Rt),x(Jt),x(Kd))};static \u0275dir=k({type:t,selectors:[["","ngFor","","ngForOf",""]],inputs:{ngForOf:"ngForOf",ngForTrackBy:"ngForTrackBy",ngForTemplate:"ngForTemplate"}})}return t})();function nC(t,n){t.context.$implicit=n.item}var ct=(()=>{class t{_viewContainer;_context=new Jd;_thenTemplateRef=null;_elseTemplateRef=null;_thenViewRef=null;_elseViewRef=null;constructor(e,i){this._viewContainer=e,this._thenTemplateRef=i}set ngIf(e){this._context.$implicit=this._context.ngIf=e,this._updateView()}set ngIfThen(e){iC(e,!1),this._thenTemplateRef=e,this._thenViewRef=null,this._updateView()}set ngIfElse(e){iC(e,!1),this._elseTemplateRef=e,this._elseViewRef=null,this._updateView()}_updateView(){this._context.$implicit?this._thenViewRef||(this._viewContainer.clear(),this._elseViewRef=null,this._thenTemplateRef&&(this._thenViewRef=this._viewContainer.createEmbeddedView(this._thenTemplateRef,this._context))):this._elseViewRef||(this._viewContainer.clear(),this._thenViewRef=null,this._elseTemplateRef&&(this._elseViewRef=this._viewContainer.createEmbeddedView(this._elseTemplateRef,this._context)))}static ngIfUseIfTypeGuard;static ngTemplateGuard_ngIf;static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||t)(x(Rt),x(Jt))};static \u0275dir=k({type:t,selectors:[["","ngIf",""]],inputs:{ngIf:"ngIf",ngIfThen:"ngIfThen",ngIfElse:"ngIfElse"}})}return t})(),Jd=class{$implicit=null;ngIf=null};function iC(t,n){if(t&&!t.createEmbeddedView)throw new D(2020,!1)}var Ua=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=u(ae);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||t)(x(Rt))};static \u0275dir=k({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[tt]})}return t})();var it=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({})}return t})();function Ha(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===n)return decodeURIComponent(o)}return null}var Bm="browser";function rC(t){return t===Bm}var za=class{_doc;constructor(n){this._doc=n}manager},eu=(()=>{class t extends za{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||t)(M(U))};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),iu=new y(""),$m=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(s=>{s.manager=this});let r=e.filter(s=>!(s instanceof eu));this._plugins=r.slice().reverse();let o=e.find(s=>s instanceof eu);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new D(-5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||t)(M(iu),M(O))};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),Um="ng-app-id";function oC(t){for(let n of t)n.remove()}function sC(t,n){let e=n.createElement("style");return e.textContent=t,e}function rR(t,n,e,i){let r=t.head?.querySelectorAll(`style[${Um}="${n}"],link[${Um}="${n}"]`);if(!r||r.length===0)return!1;for(let o of r)o.removeAttribute(Um),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]});return!0}function zm(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var Gm=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,rR(e,i,this.inline,this.external)&&this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,sC);i?.forEach(r=>this.addUsage(r,this.external,zm))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(oC(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])oC(e);this.hosts.clear()}addHost(e){if(!this.hosts.has(e)){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,sC(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,zm(i,this.doc)))}}removeHost(e){this.hosts.delete(e);for(let i of[...this.inline.values(),...this.external.values()]){let r=[];for(let o of i.elements)o.parentNode===e?o.remove():r.push(o);i.elements=r}}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||t)(M(U),M(Zr),M(Qr,8),M(Kr))};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),Hm={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Wm=/%COMP%/g;var cC="%COMP%",oR=`_nghost-${cC}`,sR=`_ngcontent-${cC}`,aR=!0,cR=new y("",{factory:()=>aR});function lR(t){return sR.replace(Wm,t)}function dR(t){return oR.replace(Wm,t)}function lC(t,n){return n.map(e=>e.replace(Wm,t))}var qm=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,s,a,c=null,l=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new $a(e,s,a,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof nu?r.applyToHost(e):r instanceof Ga&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case $n.Emulated:o=new nu(c,l,i,this.appId,d,s,a,f);break;case $n.ShadowDom:return new tu(c,e,i,s,a,this.nonce,f,l);case $n.ExperimentalIsolatedShadowDom:return new tu(c,e,i,s,a,this.nonce,f);default:o=new Ga(c,l,i,d,s,a,f);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||t)(M($m),M(io),M(Zr),M(cR),M(U),M(O),M(Qr),M(ii,8))};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})(),$a=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,i,r){this.eventManager=n,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(Hm[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(aC(n)?n.content:n).appendChild(e)}insertBefore(n,e,i){n&&(aC(n)?n.content:n).insertBefore(e,i)}removeChild(n,e){e.remove()}selectRootElement(n,e){let i=typeof n=="string"?this.doc.querySelector(n):n;if(!i)throw new D(-5104,!1);return e||(i.textContent=""),i}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,i,r){if(r){e=r+":"+e;let o=Hm[r];o?n.setAttributeNS(o,e,i):n.setAttribute(e,i)}else n.setAttribute(e,i)}removeAttribute(n,e,i){if(i){let r=Hm[i];r?n.removeAttributeNS(r,e):n.removeAttribute(`${i}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,i,r){r&(ni.DashCase|ni.Important)?n.style.setProperty(e,i,r&ni.Important?"important":""):n.style[e]=i}removeStyle(n,e,i){i&ni.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,i){n!=null&&(n[e]=i)}setValue(n,e){n.nodeValue=e}listen(n,e,i,r){if(typeof n=="string"&&(n=In().getGlobalEventTarget(this.doc,n),!n))throw new D(-5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,r)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function aC(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var tu=class extends $a{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,i,r,o,s,a,c){super(n,r,o,a),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=lC(i.id,l);for(let f of l){let m=document.createElement("style");s&&m.setAttribute("nonce",s),m.textContent=f,this.shadowRoot.appendChild(m)}let d=i.getExternalStyles?.();if(d)for(let f of d){let m=zm(f,r);s&&m.setAttribute("nonce",s),this.shadowRoot.appendChild(m)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,i){return super.insertBefore(this.nodeOrShadowRoot(n),e,i)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Ga=class extends $a{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,i,r,o,s,a,c){super(n,o,s,a),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?lC(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&eo.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},nu=class extends Ga{contentAttr;hostAttr;constructor(n,e,i,r,o,s,a,c){let l=r+"-"+i.id;super(n,e,i,o,s,a,c,l),this.contentAttr=lR(l),this.hostAttr=dR(l)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let i=super.createElement(n,e);return super.setAttribute(i,this.contentAttr,""),i}};var ru=class t extends Ba{supportsDOMEvents=!0;static makeCurrent(){jm(new t)}onAndCancel(n,e,i,r){return n.addEventListener(e,i,r),()=>{n.removeEventListener(e,i,r)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=uR();return e==null?null:fR(e)}resetBaseElement(){Wa=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return Ha(document.cookie,n)}},Wa=null;function uR(){return Wa=Wa||document.head.querySelector("base"),Wa?Wa.getAttribute("href"):null}function fR(t){return new URL(t,document.baseURI).pathname}var dC=["alt","control","meta","shift"],hR={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},pR={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},uC=(()=>{class t extends za{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,i,r,o){let s=t.parseEventName(i),a=t.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>In().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=t._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),dC.forEach(l=>{let d=i.indexOf(l);d>-1&&(i.splice(d,1),s+=l+".")}),s+=o,i.length!=0||o.length===0)return null;let c={};return c.domEventName=r,c.fullKey=s,c}static matchEventFullKeyCode(e,i){let r=hR[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),dC.forEach(s=>{if(s!==r){let a=pR[s];a(e)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{t.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||t)(M(U))};static \u0275prov=V({token:t,factory:t.\u0275fac})}return t})();function Ym(t,n,e){return De(this,null,function*(){let i=C({rootComponent:t},mR(n,e));return Y0(i)})}function mR(t,n){return{platformRef:n?.platformRef,appProviders:[...bR,...t?.providers??[]],platformProviders:yR}}function gR(){ru.makeCurrent()}function vR(){return new Wt}function _R(){return Bp(document),document}var yR=[{provide:Kr,useValue:Bm},{provide:id,useValue:gR,multi:!0},{provide:U,useFactory:_R}];var bR=[{provide:fa,useValue:"root"},{provide:Wt,useFactory:vR},{provide:iu,useClass:eu,multi:!0},{provide:iu,useClass:uC,multi:!0},qm,{provide:io,useClass:Gm},{provide:Gm,useExisting:io},$m,{provide:et,useExisting:qm},[]];var Ri=class t{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(n){n?typeof n=="string"?this.lazyInit=()=>{this.headers=new Map,n.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&n instanceof Headers?(this.headers=new Map,n.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(n).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(n){return this.init(),this.headers.has(n.toLowerCase())}get(n){this.init();let e=this.headers.get(n.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(n){return this.init(),this.headers.get(n.toLowerCase())||null}append(n,e){return this.clone({name:n,value:e,op:"a"})}set(n,e){return this.clone({name:n,value:e,op:"s"})}delete(n,e){return this.clone({name:n,value:e,op:"d"})}maybeSetNormalizedName(n,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,n)}init(){this.lazyInit&&(this.lazyInit instanceof t?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(n=>this.applyUpdate(n)),this.lazyUpdate=null))}copyFrom(n){n.init(),Array.from(n.headers.keys()).forEach(e=>{this.headers.set(e,n.headers.get(e)),this.normalizedNames.set(e,n.normalizedNames.get(e))})}clone(n){let e=new t;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof t?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([n]),e}applyUpdate(n){let e=n.name.toLowerCase();switch(n.op){case"a":case"s":let i=n.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(n.name,e);let r=(n.op==="a"?this.headers.get(e):void 0)||[];r.push(...i),this.headers.set(e,r);break;case"d":let o=n.value;if(!o)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=this.headers.get(e);if(!s)return;s=s.filter(a=>o.indexOf(a)===-1),s.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,s)}break}}addHeaderEntry(n,e){let i=n.toLowerCase();this.maybeSetNormalizedName(n,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(n,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=n.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(n,r)}forEach(n){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>n(this.normalizedNames.get(e),this.headers.get(e)))}};var su=class{map=new Map;set(n,e){return this.map.set(n,e),this}get(n){return this.map.has(n)||this.map.set(n,n.defaultValue()),this.map.get(n)}delete(n){return this.map.delete(n),this}has(n){return this.map.has(n)}keys(){return this.map.keys()}},au=class{encodeKey(n){return fC(n)}encodeValue(n){return fC(n)}decodeKey(n){return decodeURIComponent(n)}decodeValue(n){return decodeURIComponent(n)}};function CR(t,n){let e=new Map;return t.length>0&&t.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[n.decodeKey(r),""]:[n.decodeKey(r.slice(0,o)),n.decodeValue(r.slice(o+1))],c=e.get(s)||[];c.push(a),e.set(s,c)}),e}var ER=/%(\d[a-f0-9])/gi,wR={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function fC(t){return encodeURIComponent(t).replace(ER,(n,e)=>wR[e]??n)}function ou(t){return`${t}`}var Ai=class t{map;encoder;updates=null;cloneFrom=null;constructor(n={}){if(this.encoder=n.encoder||new au,n.fromString){if(n.fromObject)throw new D(2805,!1);this.map=CR(n.fromString,this.encoder)}else n.fromObject?(this.map=new Map,Object.keys(n.fromObject).forEach(e=>{let i=n.fromObject[e],r=Array.isArray(i)?i.map(ou):[ou(i)];this.map.set(e,r)})):this.map=null}has(n){return this.init(),this.map.has(n)}get(n){this.init();let e=this.map.get(n);return e?e[0]:null}getAll(n){return this.init(),this.map.get(n)||null}keys(){return this.init(),Array.from(this.map.keys())}append(n,e){return this.clone({param:n,value:e,op:"a"})}appendAll(n){let e=[];return Object.keys(n).forEach(i=>{let r=n[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(n,e){return this.clone({param:n,value:e,op:"s"})}delete(n,e){return this.clone({param:n,value:e,op:"d"})}toString(){return this.init(),this.keys().map(n=>{let e=this.encoder.encodeKey(n);return this.map.get(n).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(n=>n!=="").join("&")}clone(n){let e=new t({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(n),e}init(){this.map===null&&(this.map=new Map),this.cloneFrom!==null&&(this.cloneFrom.init(),this.cloneFrom.keys().forEach(n=>this.map.set(n,this.cloneFrom.map.get(n))),this.updates.forEach(n=>{switch(n.op){case"a":case"s":let e=(n.op==="a"?this.map.get(n.param):void 0)||[];e.push(ou(n.value)),this.map.set(n.param,e);break;case"d":if(n.value!==void 0){let i=this.map.get(n.param)||[],r=i.indexOf(ou(n.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(n.param,i):this.map.delete(n.param)}else{this.map.delete(n.param);break}}}),this.cloneFrom=this.updates=null)}};function DR(t){switch(t){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function hC(t){return typeof ArrayBuffer<"u"&&t instanceof ArrayBuffer}function pC(t){return typeof Blob<"u"&&t instanceof Blob}function mC(t){return typeof FormData<"u"&&t instanceof FormData}function xR(t){return typeof URLSearchParams<"u"&&t instanceof URLSearchParams}var Zm="Content-Type",gC="Accept",_C="text/plain",yC="application/json",IR=`${yC}, ${_C}, */*`,ls=class t{url;body=null;headers;context;reportProgress=!1;reportUploadProgress=!1;reportDownloadProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(n,e,i,r){this.url=e,this.method=n.toUpperCase();let o;if(DR(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.reportUploadProgress=!!o.reportUploadProgress,this.reportDownloadProgress=!!o.reportDownloadProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new D(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new Ri,this.context??=new su,!this.params)this.params=new Ai,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e,c="",l=e.indexOf("#");l!==-1&&(c=e.substring(l),a=e.substring(0,l));let d=a.indexOf("?"),f=d===-1?"?":d<a.length-1?"&":"";this.urlWithParams=a+f+s+c}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||hC(this.body)||pC(this.body)||mC(this.body)||xR(this.body)?this.body:this.body instanceof Ai?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||mC(this.body)?null:pC(this.body)?this.body.type||null:hC(this.body)?null:typeof this.body=="string"?_C:this.body instanceof Ai?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?yC:null}clone(n={}){let e=n.method||this.method,i=n.url||this.url,r=n.responseType||this.responseType,o=n.keepalive??this.keepalive,s=n.priority||this.priority,a=n.cache||this.cache,c=n.mode||this.mode,l=n.redirect||this.redirect,d=n.credentials||this.credentials,f=n.referrer??this.referrer,m=n.integrity||this.integrity,g=n.referrerPolicy||this.referrerPolicy,b=n.transferCache??this.transferCache,I=n.timeout??this.timeout,N=n.body!==void 0?n.body:this.body,ie=n.withCredentials??this.withCredentials,ot=n.reportProgress??this.reportProgress,Mt=n.reportUploadProgress??this.reportUploadProgress,Io=n.reportDownloadProgress??this.reportDownloadProgress,zs=n.headers||this.headers,Cr=n.params||this.params,Wc=n.context??this.context;return n.setHeaders!==void 0&&(zs=Object.keys(n.setHeaders).reduce((So,Er)=>So.set(Er,n.setHeaders[Er]),zs)),n.setParams&&(Cr=Object.keys(n.setParams).reduce((So,Er)=>So.set(Er,n.setParams[Er]),Cr)),new t(e,i,N,{params:Cr,headers:zs,context:Wc,reportProgress:ot,reportUploadProgress:Mt,reportDownloadProgress:Io,responseType:r,withCredentials:ie,transferCache:b,keepalive:o,cache:a,priority:s,timeout:I,mode:c,redirect:l,credentials:d,referrer:f,integrity:m,referrerPolicy:g})}},ds=(function(t){return t[t.Sent=0]="Sent",t[t.UploadProgress=1]="UploadProgress",t[t.ResponseHeader=2]="ResponseHeader",t[t.DownloadProgress=3]="DownloadProgress",t[t.Response=4]="Response",t[t.User=5]="User",t})(ds||{}),us=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(n,e=200,i="OK"){this.headers=n.headers||new Ri,this.status=n.status!==void 0?n.status:e,this.statusText=n.statusText||i,this.url=n.url||null,this.redirected=n.redirected,this.responseType=n.responseType,this.ok=this.status>=200&&this.status<300}},cu=class t extends us{constructor(n={}){super(n)}type=ds.ResponseHeader;clone(n={}){return new t({headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0})}},qa=class t extends us{body;constructor(n={}){super(n),this.body=n.body!==void 0?n.body:null}type=ds.Response;clone(n={}){return new t({body:n.body!==void 0?n.body:this.body,headers:n.headers||this.headers,status:n.status!==void 0?n.status:this.status,statusText:n.statusText||this.statusText,url:n.url||this.url||void 0,redirected:n.redirected??this.redirected,responseType:n.responseType??this.responseType})}},so=class extends us{name="HttpErrorResponse";message;error;ok=!1;constructor(n){super(n,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${n.url||"(unknown url)"}`:this.message=`Http failure response for ${n.url||"(unknown url)"}: ${n.status} ${n.statusText}`,this.error=n.error||null}},SR=200;var MR=/^\)\]\}',?\n/,Rq=1024*1024,bC=new y("",{factory:()=>null}),lu=(()=>{class t{fetchImpl=u(Qm,{optional:!0})?.fetch??((...e)=>globalThis.fetch(...e));ngZone=u(O);destroyRef=u(Et);maxResponseSize=u(bC);handle(e){return new re(i=>{let r=new AbortController;this.doRequest(e,r.signal,i).then(Xm,s=>i.error(new so({error:s})));let o;return e.timeout&&(o=this.ngZone.runOutsideAngular(()=>setTimeout(()=>{r.signal.aborted||r.abort(new DOMException("signal timed out","TimeoutError"))},e.timeout))),()=>{o!==void 0&&clearTimeout(o),r.abort()}})}doRequest(e,i,r){return De(this,null,function*(){let o=this.createRequestInit(e),s;try{let N=this.ngZone.runOutsideAngular(()=>this.fetchImpl(e.urlWithParams,C({signal:i},o)));TR(N),r.next({type:ds.Sent}),s=yield N}catch(N){r.error(new so({error:N,status:N.status??0,statusText:N.statusText,url:e.urlWithParams,headers:N.headers}));return}let a=new Ri(s.headers),c=s.statusText,l=s.url||e.urlWithParams,d=s.status,f=null,m=e.reportProgress||e.reportDownloadProgress;if(m&&r.next(new cu({headers:a,status:d,statusText:c,url:l})),s.body){let N=s.headers.get("content-length"),ie=N!==null?Number(N):NaN;this.maxResponseSize!==null&&Number.isFinite(ie)&&ie>this.maxResponseSize&&vC(this.maxResponseSize);let ot=[],Mt=s.body.getReader(),Io=0,zs,Cr,Wc=typeof Zone<"u"&&Zone.current,So=!1;if(yield this.ngZone.runOutsideAngular(()=>De(this,null,function*(){for(;;){if(this.destroyRef.destroyed){yield Mt.cancel(),So=!0;break}let{done:$s,value:Tf}=yield Mt.read();if($s)break;if(ot.push(Tf),Io+=Tf.length,this.maxResponseSize!==null&&Io>this.maxResponseSize&&(yield Mt.cancel(),vC(this.maxResponseSize)),m){Cr=e.responseType==="text"?(Cr??"")+(zs??=new TextDecoder).decode(Tf,{stream:!0}):void 0;let yv=()=>r.next({type:ds.DownloadProgress,total:Number.isFinite(ie)?ie:void 0,loaded:Io,partialText:Cr});Wc?Wc.run(yv):yv()}}})),So){r.complete();return}let Er=this.concatChunks(ot,Io);try{let $s=s.headers.get(Zm)??"";f=this.parseBody(e,Er,$s,d)}catch($s){r.error(new so({error:$s,headers:new Ri(s.headers),status:s.status,statusText:s.statusText,url:s.url||e.urlWithParams}));return}}d===0&&(d=f?SR:0);let g=d>=200&&d<300,b=s.redirected,I=s.type;g?(r.next(new qa({body:f,headers:a,status:d,statusText:c,url:l,redirected:b,responseType:I})),r.complete()):r.error(new so({error:f,headers:a,status:d,statusText:c,url:l,redirected:b,responseType:I}))})}parseBody(e,i,r,o){switch(e.responseType){case"json":let s=new TextDecoder().decode(i).replace(MR,"");if(s==="")return null;try{return JSON.parse(s)}catch(a){if(o<200||o>=300)return s;throw a}case"text":return new TextDecoder().decode(i);case"blob":return new Blob([i],{type:r});case"arraybuffer":return i.buffer}}createRequestInit(e){if(e.reportUploadProgress)throw new D(2824,!1);let i={},r;if(r=e.credentials,e.withCredentials&&(r="include"),e.headers.forEach((o,s)=>i[o]=s.join(",")),e.headers.has(gC)||(i[gC]=IR),!e.headers.has(Zm)){let o=e.detectContentTypeHeader();o!==null&&(i[Zm]=o)}return{body:e.serializeBody(),method:e.method,headers:i,credentials:r,keepalive:e.keepalive,cache:e.cache,priority:e.priority,mode:e.mode,redirect:e.redirect,referrer:e.referrer,integrity:e.integrity,referrerPolicy:e.referrerPolicy}}concatChunks(e,i){let r=new Uint8Array(i),o=0;for(let s of e)r.set(s,o),o+=s.length;return r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),Qm=class{};function Xm(){}function TR(t){t.then(Xm,Xm)}function vC(t){throw new D(-2825,!1)}function AR(t,n){return n(t)}function RR(t,n,e){return(i,r)=>ut(e,()=>n(i,o=>t(o,r)))}var Jm=new y("",{factory:()=>[]}),CC=new y(""),EC=new y("",{factory:()=>!0});var eg=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=M(lu),r},providedIn:"root"})}return t})();var du=(()=>{class t{backend;injector;chain=null;pendingTasks=u(ya);contributeToStability=u(EC);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=Array.from(new Set([...this.injector.get(Jm),...this.injector.get(CC,[])]));this.chain=i.reduceRight((r,o)=>RR(r,o,this.injector),AR)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(Fr(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||t)(M(eg),M(xe))};static \u0275prov=V({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),tg=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=M(du),r},providedIn:"root"})}return t})();function Km(t,n){return C({body:n},t)}var Sn=(()=>{class t{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof ls)o=e;else{let c;r.headers instanceof Ri?c=r.headers:c=new Ri(r.headers);let l;r.params&&(r.params instanceof Ai?l=r.params:l=new Ai({fromObject:r.params})),o=new ls(e,i,r.body!==void 0?r.body:null,{headers:c,context:r.context,params:l,reportProgress:r.reportProgress,reportUploadProgress:r.reportUploadProgress,reportDownloadProgress:r.reportDownloadProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=L(o).pipe(Vo(c=>this.handler.handle(c)));if(e instanceof ls||r.observe==="events")return s;let a=s.pipe(Se(c=>c instanceof qa));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(ne(c=>{if(c.body!==null&&!(c.body instanceof ArrayBuffer))throw new D(2806,!1);return c.body}));case"blob":return a.pipe(ne(c=>{if(c.body!==null&&!(c.body instanceof Blob))throw new D(2807,!1);return c.body}));case"text":return a.pipe(ne(c=>{if(c.body!==null&&typeof c.body!="string")throw new D(2808,!1);return c.body}));default:return a.pipe(ne(c=>c.body))}case"response":return a;default:throw new D(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new Ai().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,Km(r,i))}post(e,i,r={}){return this.request("POST",e,Km(r,i))}put(e,i,r={}){return this.request("PUT",e,Km(r,i))}static \u0275fac=function(i){return new(i||t)(M(tg))};static \u0275prov=V({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var NR=new y("",{factory:()=>!0}),kR="XSRF-TOKEN",OR=new y("",{factory:()=>kR}),FR="X-XSRF-TOKEN",PR=new y("",{factory:()=>FR}),LR=(()=>{class t{cookieName=u(OR);doc=u(U);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=Ha(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),wC=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=M(LR),r},providedIn:"root"})}return t})();function VR(t,n){if(!u(NR)||t.method==="GET"||t.method==="HEAD")return n(t);try{let r=u(as).href,{origin:o}=new URL(r),{origin:s}=new URL(t.url,o);if(o!==s)return n(t)}catch(r){return n(t)}let e=u(wC).getToken(),i=u(PR);return e!=null&&!t.headers.has(i)&&(t=t.clone({headers:t.headers.set(i,e)})),n(t)}var ng=(function(t){return t[t.Interceptors=0]="Interceptors",t[t.LegacyInterceptors=1]="LegacyInterceptors",t[t.CustomXsrfConfiguration=2]="CustomXsrfConfiguration",t[t.NoXsrfProtection=3]="NoXsrfProtection",t[t.JsonpSupport=4]="JsonpSupport",t[t.RequestsMadeViaParent=5]="RequestsMadeViaParent",t[t.Fetch=6]="Fetch",t[t.Xhr=7]="Xhr",t})(ng||{});function jR(t,n){return{\u0275kind:t,\u0275providers:n}}function ig(...t){let n=[Sn,lu,du,{provide:tg,useExisting:du},{provide:eg,useFactory:()=>u(lu)},{provide:Jm,useValue:VR,multi:!0}];for(let e of t)n.push(...e.\u0275providers);return _i(n)}function rg(t){return jR(ng.Interceptors,t.map(n=>({provide:Jm,useValue:n,multi:!0})))}var DC=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(i){return new(i||t)(M(U))};static \u0275prov=V({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Za=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=V({token:t,factory:function(i){let r=null;return i?r=new(i||t):r=M(BR),r},providedIn:"root"})}return t})(),BR=(()=>{class t extends Za{_doc=u(U);sanitize(e,i){if(i==null)return null;switch(e){case at.NONE:return i;case at.HTML:return Ii(i,"HTML")?Wn(i):qp(this._doc,String(i)).toString();case at.STYLE:return Ii(i,"Style")?Wn(i):i;case at.SCRIPT:if(Ii(i,"Script"))return Wn(i);throw new D(5200,!1);case at.URL:return Ii(i,"URL")?Wn(i):Aa(String(i));case at.RESOURCE_URL:if(Ii(i,"ResourceURL"))return Wn(i);throw new D(5201,!1);default:throw new D(5202,!1)}}bypassSecurityTrustHtml(e){return Hp(e)}bypassSecurityTrustStyle(e){return zp(e)}bypassSecurityTrustScript(e){return $p(e)}bypassSecurityTrustUrl(e){return Gp(e)}bypassSecurityTrustResourceUrl(e){return Wp(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var X="primary",lc=Symbol("RouteTitle"),lg=class{params;constructor(n){this.params=n||{}}has(n){return Object.prototype.hasOwnProperty.call(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function co(t){return new lg(t)}function og(t,n,e){for(let i=0;i<t.length;i++){let r=t[i],o=n[i];if(r[0]===":")e[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function OC(t,n,e){let i=e.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||i.length<t.length))return null;let c={},l=t.slice(0,i.length);return og(i,l,c)?{consumed:l,posParams:c}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let a={};return!og(o,t.slice(0,o.length),a)||!og(s,t.slice(t.length-s.length),a)?null:{consumed:t,posParams:a}}function gu(t){return new Promise((n,e)=>{t.pipe(pi()).subscribe({next:i=>n(i),error:i=>e(i)})})}function UR(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!si(t[e],n[e]))return!1;return!0}function si(t,n){let e=t?dg(t):void 0,i=n?dg(n):void 0;if(!e||!i||e.length!=i.length)return!1;let r;for(let o=0;o<e.length;o++)if(r=e[o],!FC(t[r],n[r]))return!1;return!0}function dg(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function FC(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),i=[...n].sort();return e.every((r,o)=>i[o]===r)}else return t===n}function HR(t){return t.length>0?t[t.length-1]:null}function fo(t){return ea(t)?t:tr(t)?Le(Promise.resolve(t)):L(t)}function PC(t){return ea(t)?gu(t):Promise.resolve(t)}var zR={exact:jC,subset:BC},LC={exact:$R,subset:GR,ignored:()=>!0},VC={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},ug={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function IC(t,n,e){return zR[e.paths](t.root,n.root,e.matrixParams)&&LC[e.queryParams](t.queryParams,n.queryParams)&&!(e.fragment==="exact"&&t.fragment!==n.fragment)}function $R(t,n){return si(t,n)}function jC(t,n,e){if(!ao(t.segments,n.segments)||!hu(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let i in n.children)if(!t.children[i]||!jC(t.children[i],n.children[i],e))return!1;return!0}function GR(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>FC(t[e],n[e]))}function BC(t,n,e){return UC(t,n,n.segments,e)}function UC(t,n,e,i){if(t.segments.length>e.length){let r=t.segments.slice(0,e.length);return!(!ao(r,e)||n.hasChildren()||!hu(r,e,i))}else if(t.segments.length===e.length){if(!ao(t.segments,e)||!hu(t.segments,e,i))return!1;for(let r in n.children)if(!t.children[r]||!BC(t.children[r],n.children[r],i))return!1;return!0}else{let r=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!ao(t.segments,r)||!hu(t.segments,r,i)||!t.children[X]?!1:UC(t.children[X],n,o,i)}}function hu(t,n,e){return n.every((i,r)=>LC[e](t[r].parameters,i.parameters))}var ln=class{root;queryParams;fragment;_queryParamMap;constructor(n=new Ce([],{}),e={},i=null){this.root=n,this.queryParams=e,this.fragment=i}get queryParamMap(){return this._queryParamMap??=co(this.queryParams),this._queryParamMap}toString(){return YR.serialize(this)}},Ce=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return pu(this)}},or=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=co(this.parameters),this._parameterMap}toString(){return zC(this)}};function WR(t,n){return ao(t,n)&&t.every((e,i)=>si(e.parameters,n[i].parameters))}function ao(t,n){return t.length!==n.length?!1:t.every((e,i)=>e.path===n[i].path)}function qR(t,n){let e=[];return Object.entries(t.children).forEach(([i,r])=>{i===X&&(e=e.concat(n(r,i)))}),Object.entries(t.children).forEach(([i,r])=>{i!==X&&(e=e.concat(n(r,i)))}),e}var bs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>new sr})}return t})(),sr=class{parse(n){let e=new hg(n);return new ln(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${Ka(n.root,!0)}`,i=QR(n.queryParams),r=typeof n.fragment=="string"?`#${ZR(n.fragment)}`:"";return`${e}${i}${r}`}},YR=new sr;function pu(t){return t.segments.map(n=>zC(n)).join("/")}function Ka(t,n){if(!t.hasChildren())return pu(t);if(n){let e=t.children[X]?Ka(t.children[X],!1):"",i=[];return Object.entries(t.children).forEach(([r,o])=>{r!==X&&i.push(`${r}:${Ka(o,!1)}`)}),i.length>0?`${e}(${i.join("//")})`:e}else{let e=qR(t,(i,r)=>r===X?[Ka(t.children[X],!1)]:[`${r}:${Ka(i,!1)}`]);return Object.keys(t.children).length===1&&t.children[X]!=null?`${pu(t)}/${e[0]}`:`${pu(t)}/(${e.join("//")})`}}function HC(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function uu(t){return HC(t).replace(/%3B/gi,";")}function ZR(t){return encodeURI(t)}function fg(t){return HC(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function mu(t){return decodeURIComponent(t)}function SC(t){return mu(t.replace(/\+/g,"%20"))}function zC(t){return`${fg(t.path)}${KR(t.parameters)}`}function KR(t){return Object.entries(t).map(([n,e])=>`;${fg(n)}=${fg(e)}`).join("")}function QR(t){let n=Object.entries(t).map(([e,i])=>Array.isArray(i)?i.map(r=>`${uu(e)}=${uu(r)}`).join("&"):`${uu(e)}=${uu(i)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var XR=/^[^\/()?;#]+/;function sg(t){let n=t.match(XR);return n?n[0]:""}var JR=/^[^\/()?;=#]+/;function eN(t){let n=t.match(JR);return n?n[0]:""}var tN=/^[^=?&#]+/;function nN(t){let n=t.match(tN);return n?n[0]:""}var iN=/^[^&#]+/;function rN(t){let n=t.match(iN);return n?n[0]:""}var hg=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new Ce([],{}):new Ce([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new D(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,n));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,n)),(e.length>0||Object.keys(i).length>0)&&(r[X]=new Ce(e,i)),r}parseSegment(){let n=sg(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new D(4009,!1);return this.capture(n),new or(mu(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=eN(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let r=sg(this.remaining);r&&(i=r,this.capture(i))}n[mu(e)]=mu(i)}parseQueryParam(n){let e=nN(this.remaining);if(!e)return;this.capture(e);let i="";if(this.consumeOptional("=")){let s=rN(this.remaining);s&&(i=s,this.capture(i))}let r=SC(e),o=SC(i);if(n.hasOwnProperty(r)){let s=n[r];Array.isArray(s)||(s=[s],n[r]=s),s.push(o)}else n[r]=o}parseParens(n,e){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=sg(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new D(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):n&&(s=X);let a=this.parseChildren(e+1);i[s??X]=Object.keys(a).length===1&&a[X]?a[X]:new Ce([],a),this.consumeOptional("//")}return i}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new D(4011,!1)}};function $C(t){return t.segments.length>0?new Ce([],{[X]:t}):t}function GC(t){let n={};for(let[i,r]of Object.entries(t.children)){let o=GC(r);if(i===X&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))n[s]=a;else(o.segments.length>0||o.hasChildren())&&(n[i]=o)}let e=new Ce(t.segments,n);return oN(e)}function oN(t){if(t.numberOfChildren===1&&t.children[X]){let n=t.children[X];return new Ce(t.segments.concat(n.segments),n.children)}return t}function ar(t){return t instanceof ln}function WC(t,n,e=null,i=null,r=new sr){let o=qC(t);return YC(o,n,e,i,r)}function qC(t){let n;function e(o){let s={};for(let c of o.children){let l=e(c);s[c.outlet]=l}let a=new Ce(o.url,s);return o===t&&(n=a),a}let i=e(t.root),r=$C(i);return n??r}function YC(t,n,e,i,r){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return ag(o,o,o,e,i,r);let s=sN(n);if(s.toRoot())return ag(o,o,new Ce([],{}),e,i,r);let a=aN(s,o,t),c=a.processChildren?Xa(a.segmentGroup,a.index,s.commands):KC(a.segmentGroup,a.index,s.commands);return ag(o,a.segmentGroup,c,e,i,r)}function vu(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function tc(t){return typeof t=="object"&&t!=null&&t.outlets}function MC(t,n,e){t||="\u0275";let i=new ln;return i.queryParams={[t]:n},e.parse(e.serialize(i)).queryParams[t]}function ag(t,n,e,i,r,o){let s={};for(let[l,d]of Object.entries(i??{}))s[l]=Array.isArray(d)?d.map(f=>MC(l,f,o)):MC(l,d,o);let a;t===n?a=e:a=ZC(t,n,e);let c=$C(GC(a));return new ln(c,s,r)}function ZC(t,n,e){let i={};return Object.entries(t.children).forEach(([r,o])=>{o===n?i[r]=e:i[r]=ZC(o,n,e)}),new Ce(t.segments,i)}var _u=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,i){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=i,n&&i.length>0&&vu(i[0]))throw new D(4003,!1);let r=i.find(tc);if(r&&r!==HR(i))throw new D(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function sN(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new _u(!0,0,t);let n=0,e=!1,i=t.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?e=!0:a===".."?n++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new _u(e,n,i)}var hs=class{segmentGroup;processChildren;index;constructor(n,e,i){this.segmentGroup=n,this.processChildren=e,this.index=i}};function aN(t,n,e){if(t.isAbsolute)return new hs(n,!0,0);if(!e)return new hs(n,!1,NaN);if(e.parent===null)return new hs(e,!0,0);let i=vu(t.commands[0])?0:1,r=e.segments.length-1+i;return cN(e,r,t.numberOfDoubleDots)}function cN(t,n,e){let i=t,r=n,o=e;for(;o>r;){if(o-=r,i=i.parent,!i)throw new D(4005,!1);r=i.segments.length}return new hs(i,!1,r-o)}function lN(t){return tc(t[0])?t[0].outlets:{[X]:t}}function KC(t,n,e){if(t??=new Ce([],{}),t.segments.length===0&&t.hasChildren())return Xa(t,n,e);let i=dN(t,n,e),r=e.slice(i.commandIndex);if(i.match&&i.pathIndex<t.segments.length){let o=new Ce(t.segments.slice(0,i.pathIndex),{});return o.children[X]=new Ce(t.segments.slice(i.pathIndex),t.children),Xa(o,0,r)}else return i.match&&r.length===0?new Ce(t.segments,{}):i.match&&!t.hasChildren()?pg(t,n,e):i.match?Xa(t,0,r):pg(t,n,e)}function Xa(t,n,e){if(e.length===0)return new Ce(t.segments,{});{let i=lN(e),r={};if(Object.keys(i).some(o=>o!==X)&&t.children[X]&&t.numberOfChildren===1&&t.children[X].segments.length===0){let o=Xa(t.children[X],n,e);return new Ce(t.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=KC(t.children[o],n,s))}),Object.entries(t.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new Ce(t.segments,r)}}function dN(t,n,e){let i=0,r=n,o={match:!1,pathIndex:0,commandIndex:0};for(;r<t.segments.length;){if(i>=e.length)return o;let s=t.segments[r],a=e[i];if(tc(a))break;let c=`${a}`,l=i<e.length-1?e[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!AC(c,l,s))return o;i+=2}else{if(!AC(c,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function pg(t,n,e){let i=t.segments.slice(0,n),r=0;for(;r<e.length;){let o=e[r];if(tc(o)){let c=uN(o.outlets);return new Ce(i,c)}if(r===0&&vu(e[0])){let c=t.segments[n];i.push(new or(c.path,TC(e[0]))),r++;continue}let s=tc(o)?o.outlets[X]:`${o}`,a=r<e.length-1?e[r+1]:null;s&&a&&vu(a)?(i.push(new or(s,TC(a))),r+=2):(i.push(new or(s,{})),r++)}return new Ce(i,{})}function uN(t){let n={};return Object.entries(t).forEach(([e,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(n[e]=pg(new Ce([],{}),0,i))}),n}function TC(t){let n={};return Object.entries(t).forEach(([e,i])=>n[e]=`${i}`),n}function AC(t,n,e){return t==e.path&&si(n,e.parameters)}var Ja="imperative",ht=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(ht||{}),dn=class{id;url;constructor(n,e){this.id=n,this.url=e}},lo=class extends dn{type=ht.NavigationStart;navigationTrigger;restoredState;constructor(n,e,i="imperative",r=null){super(n,e),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},ai=class extends dn{urlAfterRedirects;type=ht.NavigationEnd;constructor(n,e,i){super(n,e),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},kt=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(kt||{}),nc=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(nc||{}),Mn=class extends dn{reason;code;type=ht.NavigationCancel;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function QC(t){return t instanceof Mn&&(t.code===kt.Redirect||t.code===kt.SupersededByNewNavigation)}var ki=class extends dn{reason;code;type=ht.NavigationSkipped;constructor(n,e,i,r){super(n,e),this.reason=i,this.code=r}},uo=class extends dn{error;target;type=ht.NavigationError;constructor(n,e,i,r){super(n,e),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},ic=class extends dn{urlAfterRedirects;state;type=ht.RoutesRecognized;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},yu=class extends dn{urlAfterRedirects;state;type=ht.GuardsCheckStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},bu=class extends dn{urlAfterRedirects;state;shouldActivate;type=ht.GuardsCheckEnd;constructor(n,e,i,r,o){super(n,e),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Cu=class extends dn{urlAfterRedirects;state;type=ht.ResolveStart;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Eu=class extends dn{urlAfterRedirects;state;type=ht.ResolveEnd;constructor(n,e,i,r){super(n,e),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},wu=class{route;type=ht.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},Du=class{route;type=ht.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},xu=class{snapshot;type=ht.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Iu=class{snapshot;type=ht.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Su=class{snapshot;type=ht.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Mu=class{snapshot;type=ht.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var ms=class{},rc=class{},gs=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function fN(t){return!(t instanceof ms)&&!(t instanceof gs)&&!(t instanceof rc)}var Tu=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new Cs(this.rootInjector)}},Cs=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,i){let r=this.getOrCreateContext(e);r.outlet=i,this.contexts.set(e,r)}onChildOutletDestroyed(e){let i=this.getContext(e);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let i=this.getContext(e);return i||(i=new Tu(this.rootInjector),this.contexts.set(e,i)),i}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(i){return new(i||t)(M(xe))};static \u0275prov=V({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),Au=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=mg(n,this._root);return e?e.children.map(i=>i.value):[]}firstChild(n){let e=mg(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=gg(n,this._root);return e.length<2?[]:e[e.length-2].children.map(r=>r.value).filter(r=>r!==n)}pathFromRoot(n){return gg(n,this._root).map(e=>e.value)}};function mg(t,n){if(t===n.value)return n;for(let e of n.children){let i=mg(t,e);if(i)return i}return null}function gg(t,n){if(t===n.value)return[n];for(let e of n.children){let i=gg(t,e);if(i.length)return i.unshift(n),i}return[]}var cn=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function fs(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var oc=class extends Au{snapshot;constructor(n,e){super(n),this.snapshot=e,xg(this,n)}toString(){return this.snapshot.toString()}};function XC(t,n){let e=hN(t,n),i=new st([new or("",{})]),r=new st({}),o=new st({}),s=new st({}),a=new st(""),c=new Yt(i,r,s,a,o,X,t,e.root);return c.snapshot=e.root,new oc(new cn(c,[]),e)}function hN(t,n){let e={},i={},r={},s=new vs([],e,r,"",i,X,t,null,{},n);return new sc("",new cn(s,[]))}var Yt=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;_localInjector;constructor(n,e,i,r,o,s,a,c){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(ne(l=>l[lc]))??L(void 0),this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(ne(n=>co(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(ne(n=>co(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}},pN="always";function Dg(t,n,e){let i,{routeConfig:r}=t;return n!==null&&(e==="always"||r?.path===""||!n.component&&!n.routeConfig?.loadComponent)?i={params:C(C({},n.params),t.params),data:C(C({},n.data),t.data),resolve:C(C(C(C({},t.data),n.data),r?.data),t._resolvedData)}:i={params:C({},t.params),data:C({},t.data),resolve:C(C({},t.data),t._resolvedData??{})},r&&eE(r)&&(i.resolve[lc]=r.title),i}var vs=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[lc]}constructor(n,e,i,r,o,s,a,c,l,d){this.url=n,this.params=e,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=d}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=co(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=co(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(i=>i.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},sc=class extends Au{url;constructor(n,e){super(e),this.url=n,xg(this,e)}toString(){return JC(this._root)}};function xg(t,n){n.value._routerState=t,n.children.forEach(e=>xg(t,e))}function JC(t){let n=t.children.length>0?` { ${t.children.map(JC).join(", ")} } `:"";return`${t.value}${n}`}function cg(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,si(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),si(n.params,e.params)||t.paramsSubject.next(e.params),UR(n.url,e.url)||t.urlSubject.next(e.url),si(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function vg(t,n){let e=si(t.params,n.params)&&WR(t.url,n.url),i=!t.parent!=!n.parent;return e&&!i&&(!t.parent||vg(t.parent,n.parent))}function eE(t){return typeof t.title=="string"||t.title===null}var tE=new y(""),dc=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=X;activateEvents=new Y;deactivateEvents=new Y;attachEvents=new Y;detachEvents=new Y;routerOutletData=ss();parentContexts=u(Cs);location=u(Rt);changeDetector=u(me);inputBinder=u(Ou,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:i,previousValue:r}=e.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new D(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new D(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new D(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,i){this.activated=e,this._activatedRoute=i,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,i){if(this.isActivated)throw new D(4013,!1);this._activatedRoute=e;let r=this.location,s=e.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new _g(e,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[tt]})}return t})(),_g=class{route;childContexts;parent;outletData;constructor(n,e,i,r){this.route=n,this.childContexts=e,this.parent=i,this.outletData=r}get(n,e){return n===Yt?this.route:n===Cs?this.childContexts:n===tE?this.outletData:this.parent.get(n,e)}},Ou=new y("");var Ig=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&pe(0,"router-outlet")},dependencies:[dc],encapsulation:2,changeDetection:1})}return t})();function Sg(t){let n=t.children&&t.children.map(Sg),e=n?H(C({},t),{children:n}):C({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==X&&(e.component=Ig),e}function mN(t,n,e){let i=new Set,r=ac(t,n._root,e?e._root:void 0,i);return{newlyCreatedRoutes:i,state:new oc(r,n)}}function ac(t,n,e,i){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let r=e.value;r._futureSnapshot=n.value;let o=gN(t,n,e,i);return new cn(r,o)}else{if(t.shouldAttach(n.value)){let s=t.retrieve(n.value);if(s!==null){let a=s.route;return a.value._futureSnapshot=n.value,a.children=n.children.map(c=>ac(t,c,void 0,i)),a}}let r=vN(n.value);i.add(r);let o=n.children.map(s=>ac(t,s,void 0,i));return new cn(r,o)}}function gN(t,n,e,i){return n.children.map(r=>{for(let o of e.children)if(t.shouldReuseRoute(r.value,o.value.snapshot))return ac(t,r,o,i);return ac(t,r,void 0,i)})}function vN(t){return new Yt(new st(t.url),new st(t.params),new st(t.queryParams),new st(t.fragment),new st(t.data),t.outlet,t.component,t)}var _s=class{redirectTo;navigationBehaviorOptions;constructor(n,e){this.redirectTo=n,this.navigationBehaviorOptions=e}},nE="ngNavigationCancelingError";function Ru(t,n){let{redirectTo:e,navigationBehaviorOptions:i}=ar(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,r=iE(!1,kt.Redirect);return r.url=e,r.navigationBehaviorOptions=i,r}function iE(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[nE]=!0,e.cancellationCode=n,e}function _N(t){return rE(t)&&ar(t.url)}function rE(t){return!!t&&t[nE]}var yg=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,i,r,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,i,n),cg(this.futureState.root),this.activateChildRoutes(e,i,n)}deactivateChildRoutes(n,e,i){let r=fs(e);n.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(n,e,s.children)}else this.deactivateChildRoutes(n,e,i);else o&&this.deactivateRouteAndItsChildren(e,i)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=fs(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(n.value.snapshot,{componentRef:s,route:n,contexts:a})}}deactivateRouteAndOutlet(n,e){let i=e.getContext(n.value.outlet),r=i&&n.value.component?i.children:e,o=fs(n);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null),n.value._localInjector?.destroy()}activateChildRoutes(n,e,i){let r=fs(e);n.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new Mu(o.value.snapshot))}),n.children.length&&this.forwardEvent(new Iu(n.value.snapshot))}activateRoutes(n,e,i){let r=n.value,o=e?e.value:null;if(cg(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(n,e,s.children)}else this.activateChildRoutes(n,e,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),cg(a.route.value),this.activateChildRoutes(n,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(n,null,s.children)}else this.activateChildRoutes(n,null,i)}},Nu=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},ps=class{component;route;constructor(n,e){this.component=n,this.route=e}};function yN(t,n,e){let i=t._root,r=n?n._root:null;return Qa(i,r,e,[i.value])}function bN(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function Es(t,n){let e=Symbol(),i=n.get(t,e);return i===e?typeof t=="function"&&!fh(t)?t:n.get(t):i}function Qa(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=fs(n);return t.children.forEach(s=>{CN(s,o[s.value.outlet],e,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>ec(a,e.getContext(s),r)),r}function CN(t,n,e,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,s=n?n.value:null,a=e?e.getContext(t.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let c=EN(s,o,o.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new Nu(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?Qa(t,n,a?a.children:null,i,r):Qa(t,n,e,i,r),c&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new ps(a.outlet.component,s))}else s&&ec(n,a,r),r.canActivateChecks.push(new Nu(i)),o.component?Qa(t,null,a?a.children:null,i,r):Qa(t,null,e,i,r);return r}function EN(t,n,e){if(typeof e=="function")return ut(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!ao(t.url,n.url);case"pathParamsOrQueryParamsChange":return!ao(t.url,n.url)||!si(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!vg(t,n)||!si(t.queryParams,n.queryParams);default:return!vg(t,n)}}function ec(t,n,e){let i=fs(t),r=t.value;Object.entries(i).forEach(([o,s])=>{r.component?n?ec(s,n.children.getContext(o),e):ec(s,null,e):ec(s,n,e)}),r.component?n&&n.outlet&&n.outlet.isActivated?e.canDeactivateChecks.push(new ps(n.outlet.component,r)):e.canDeactivateChecks.push(new ps(null,r)):e.canDeactivateChecks.push(new ps(null,r))}function uc(t){return typeof t=="function"}function wN(t){return typeof t=="boolean"}function DN(t){return t&&uc(t.canLoad)}function xN(t){return t&&uc(t.canActivate)}function IN(t){return t&&uc(t.canActivateChild)}function SN(t){return t&&uc(t.canDeactivate)}function MN(t){return t&&uc(t.canMatch)}function oE(t){return t instanceof Nr||t?.name==="EmptyError"}var fu=Symbol("INITIAL_VALUE");function ys(){return We(t=>Yf(t.map(n=>n.pipe(Gt(1),_t(fu)))).pipe(ne(n=>{for(let e of n)if(e!==!0){if(e===fu)return fu;if(e===!1||TN(e))return e}return!0}),Se(n=>n!==fu),Gt(1)))}function TN(t){return ar(t)||t instanceof _s}function sE(t){return t.aborted?L(void 0).pipe(Gt(1)):new re(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function aE(t){return ke(sE(t))}function AN(t){return $t(n=>{let{targetSnapshot:e,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=n;return o.length===0&&r.length===0?L(H(C({},n),{guardsResult:!0})):RN(o,e,i).pipe($t(s=>s&&wN(s)?NN(e,r,t):L(s)),ne(s=>H(C({},n),{guardsResult:s})))})}function RN(t,n,e){return Le(t).pipe($t(i=>LN(i.component,i.route,e,n)),pi(i=>i!==!0,!0))}function NN(t,n,e){return Le(n).pipe(Vo(i=>Lo(ON(i.route.parent,e),kN(i.route,e),PN(t,i.path),FN(t,i.route))),pi(i=>i!==!0,!0))}function kN(t,n){return t!==null&&n&&n(new Su(t)),L(!0)}function ON(t,n){return t!==null&&n&&n(new xu(t)),L(!0)}function FN(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return L(!0);let i=e.map(r=>kr(()=>{let o=n._environmentInjector,s=Es(r,o),a=xN(s)?s.canActivate(n,t):ut(o,()=>s(n,t));return fo(a).pipe(pi())}));return L(i).pipe(ys())}function PN(t,n){let e=n[n.length-1],r=n.slice(0,n.length-1).reverse().map(o=>bN(o)).filter(o=>o!==null).map(o=>kr(()=>{let s=o.guards.map(a=>{let c=o.node._environmentInjector,l=Es(a,c),d=IN(l)?l.canActivateChild(e,t):ut(c,()=>l(e,t));return fo(d).pipe(pi())});return L(s).pipe(ys())}));return L(r).pipe(ys())}function LN(t,n,e,i){let r=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!r||r.length===0)return L(!0);let o=r.map(s=>{let a=n._environmentInjector,c=Es(s,a),l=SN(c)?c.canDeactivate(t,n,e,i):ut(a,()=>c(t,n,e,i));return fo(l).pipe(pi())});return L(o).pipe(ys())}function VN(t,n,e,i,r){let o=n.canLoad;if(o===void 0||o.length===0)return L(!0);let s=o.map(a=>{let c=Es(a,t),l=DN(c)?c.canLoad(n,e):ut(t,()=>c(n,e)),d=fo(l);return r?d.pipe(aE(r)):d});return L(s).pipe(ys(),cE(i))}function cE(t){return $f(Ze(n=>{if(typeof n!="boolean")throw Ru(t,n)}),ne(n=>n===!0))}function jN(t,n,e,i,r,o){let s=n.canMatch;if(!s||s.length===0)return L(!0);let a=s.map(c=>{let l=Es(c,t),d=MN(l)?l.canMatch(n,e,r):ut(t,()=>l(n,e,r));return fo(d).pipe(aE(o))});return L(a).pipe(ys(),cE(i))}var Ni=class t extends Error{segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},cc=class t extends Error{urlTree;constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function BN(t){throw new D(4e3,!1)}function UN(t){throw iE(!1,kt.GuardRejected)}var bg=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}lineralizeSegments(n,e){return De(this,null,function*(){let i=[],r=e.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[X])throw BN(`${n.redirectTo}`);r=r.children[X]}})}applyRedirectCommands(n,e,i,r,o){return De(this,null,function*(){let s=yield HN(e,r,o);if(s instanceof ln)throw new cc(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),n,i);if(s[0]==="/")throw new cc(a);return a})}applyRedirectCreateUrlTree(n,e,i,r){let o=this.createSegmentGroup(n,e.root,i,r);return new ln(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let i={};return Object.entries(n).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=e[a]}else i[r]=o}),i}createSegmentGroup(n,e,i,r){let o=this.createSegments(n,e.segments,i,r),s={};return Object.entries(e.children).forEach(([a,c])=>{s[a]=this.createSegmentGroup(n,c,i,r)}),new Ce(o,s)}createSegments(n,e,i,r){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,r):this.findOrReturn(o,i))}findPosParam(n,e,i){let r=i[e.path.substring(1)];if(!r)throw new D(4001,!1);return r}findOrReturn(n,e){let i=0;for(let r of e){if(r.path===n.path)return e.splice(i),r;i++}return n}};function HN(t,n,e){if(typeof t=="string")return Promise.resolve(t);let i=t;return gu(fo(ut(e,()=>i(n))))}function zN(t,n){return t.providers&&!t._injector&&(t._injector=Oa(t.providers,n,`Route: ${t.path}`)),t._injector??n}function qn(t){return t.outlet||X}function $N(t,n){let e=t.filter(i=>qn(i)===n);return e.push(...t.filter(i=>qn(i)!==n)),e}var Cg={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function lE(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function GN(t,n,e,i,r,o,s){let a=dE(t,n,e);if(!a.matched)return L(a);let c=lE(o(a));return i=zN(n,i),jN(i,n,e,r,c,s).pipe(ne(l=>l===!0?a:C({},Cg)))}function dE(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?C({},Cg):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let r=(n.matcher||OC)(e,t,n);if(!r)return C({},Cg);let o={};Object.entries(r.posParams??{}).forEach(([a,c])=>{o[a]=c.path});let s=r.consumed.length>0?C(C({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:e.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function RC(t,n,e,i,r){return e.length>0&&YN(t,e,i,r)?{segmentGroup:new Ce(n,qN(i,new Ce(e,t.children))),slicedSegments:[]}:e.length===0&&ZN(t,e,i)?{segmentGroup:new Ce(t.segments,WN(t,e,i,t.children)),slicedSegments:e}:{segmentGroup:new Ce(t.segments,t.children),slicedSegments:e}}function WN(t,n,e,i){let r={};for(let o of e)if(Fu(t,n,o)&&!i[qn(o)]){let s=new Ce([],{});r[qn(o)]=s}return C(C({},i),r)}function qN(t,n){let e={};e[X]=n;for(let i of t)if(i.path===""&&qn(i)!==X){let r=new Ce([],{});e[qn(i)]=r}return e}function YN(t,n,e,i){return e.some(r=>!Fu(t,n,r)||!(qn(r)!==X)?!1:!(i!==void 0&&qn(r)===i))}function ZN(t,n,e){return e.some(i=>Fu(t,n,i))}function Fu(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function KN(t,n,e){return n.length===0&&!t.children[e]}var Eg=class{};function QN(t,n,e,i,r,o,s,a){return De(this,null,function*(){return new wg(t,n,e,i,r,s,o,a).recognize()})}var XN=31,wg=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(n,e,i,r,o,s,a,c){this.injector=n,this.configLoader=e,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new bg(this.urlSerializer,this.urlTree)}noMatchError(n){return new D(4002,`'${n.segmentGroup}'`)}recognize(){return De(this,null,function*(){let n=RC(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:i}=yield this.match(n),r=new cn(i,e),o=new sc("",r),s=WC(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}})}match(n){return De(this,null,function*(){let e=new vs([],Object.freeze({}),Object.freeze(C({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),X,this.rootComponentType,null,{},this.injector);try{return{children:yield this.processSegmentGroup(this.injector,this.config,n,X,e),rootSnapshot:e}}catch(i){if(i instanceof cc)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof Ni?this.noMatchError(i):i}})}processSegmentGroup(n,e,i,r,o){return De(this,null,function*(){if(i.segments.length===0&&i.hasChildren())return this.processChildren(n,e,i,o);let s=yield this.processSegment(n,e,i,i.segments,r,!0,o);return s instanceof cn?[s]:[]})}processChildren(n,e,i,r){return De(this,null,function*(){let o=[];for(let c of Object.keys(i.children))c==="primary"?o.unshift(c):o.push(c);let s=[];for(let c of o){let l=i.children[c],d=$N(e,c),f=yield this.processSegmentGroup(n,d,l,c,r);s.push(...f)}let a=uE(s);return JN(a),a})}processSegment(n,e,i,r,o,s,a){return De(this,null,function*(){for(let c of e)try{return yield this.processSegmentAgainstRoute(c._injector??n,e,c,i,r,o,s,a)}catch(l){if(l instanceof Ni||oE(l))continue;throw l}if(KN(i,r,o))return new Eg;throw new Ni(i)})}processSegmentAgainstRoute(n,e,i,r,o,s,a,c){return De(this,null,function*(){if(qn(i)!==s&&(s===X||!Fu(r,o,i)))throw new Ni(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,r,i,o,s,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(n,r,e,i,o,s,c);throw new Ni(r)})}expandSegmentAgainstRouteUsingRedirect(n,e,i,r,o,s,a){return De(this,null,function*(){let{matched:c,parameters:l,consumedSegments:d,positionalParamSegments:f,remainingSegments:m}=dE(e,r,o);if(!c)throw new Ni(e);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>XN&&(this.allowRedirects=!1));let g=this.createSnapshot(n,r,o,l,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let b=yield this.applyRedirects.applyRedirectCommands(d,r.redirectTo,f,lE(g),n),I=yield this.applyRedirects.lineralizeSegments(r,b);return this.processSegment(n,i,e,I.concat(m),s,!1,a)})}createSnapshot(n,e,i,r,o){let s=new vs(i,r,Object.freeze(C({},this.urlTree.queryParams)),this.urlTree.fragment,tk(e),qn(e),e.component??e._loadedComponent??null,e,nk(e),n),a=Dg(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}matchSegmentAgainstRoute(n,e,i,r,o,s){return De(this,null,function*(){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=Mt=>this.createSnapshot(n,i,Mt.consumedSegments,Mt.parameters,s),c=yield gu(GN(e,i,r,n,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(e.children={}),!c?.matched)throw new Ni(e);n=i._injector??n;let{routes:l}=yield this.getChildConfig(n,i,r),d=i._loadedInjector??n,{parameters:f,consumedSegments:m,remainingSegments:g}=c,b=this.createSnapshot(n,i,m,f,s),{segmentGroup:I,slicedSegments:N}=RC(e,m,g,l,o);if(N.length===0&&I.hasChildren()){let Mt=yield this.processChildren(d,l,I,b);return new cn(b,Mt)}if(l.length===0&&N.length===0)return new cn(b,[]);let ie=qn(i)===o,ot=yield this.processSegment(d,l,I,N,ie?X:o,!0,b);return new cn(b,ot instanceof cn?[ot]:[])})}getChildConfig(n,e,i){return De(this,null,function*(){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(yield gu(VN(n,e,i,this.urlSerializer,this.abortSignal))){let o=yield this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw UN(e)}return{routes:[],injector:n}})}};function JN(t){t.sort((n,e)=>n.value.outlet===X?-1:e.value.outlet===X?1:n.value.outlet.localeCompare(e.value.outlet))}function ek(t){let n=t.value.routeConfig;return n&&n.path===""}function uE(t){let n=[],e=new Set;for(let i of t){if(!ek(i)){n.push(i);continue}let r=n.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),e.add(r)):n.push(i)}for(let i of e){let r=uE(i.children);n.push(new cn(i.value,r))}return n.filter(i=>!e.has(i))}function tk(t){return t.data||{}}function nk(t){return t.resolve||{}}function ik(t,n,e,i,r,o,s){return $t(a=>De(null,null,function*(){let{state:c,tree:l}=yield QN(t,n,e,i,a.extractedUrl,r,o,s);return H(C({},a),{targetSnapshot:c,urlAfterRedirects:l})}))}function rk(t){return $t(n=>{let{targetSnapshot:e,guards:{canActivateChecks:i}}=n;if(!i.length)return L(n);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let c of fE(a))o.add(c);let s=0;return Le(o).pipe(Vo(a=>r.has(a)?ok(a,e,t):(a.data=Dg(a,a.parent,t).resolve,L(void 0))),Ze(()=>s++),Il(1),$t(a=>s===o.size?L(n):Ye))})}function fE(t){let n=t.children.map(e=>fE(e)).flat();return[t,...n]}function ok(t,n,e){let i=t.routeConfig,r=t._resolve;return i?.title!==void 0&&!eE(i)&&(r[lc]=i.title),kr(()=>(t.data=Dg(t,t.parent,e).resolve,sk(r,t,n).pipe(ne(o=>(t._resolvedData=o,t.data=C(C({},t.data),o),null)))))}function sk(t,n,e){let i=dg(t);if(i.length===0)return L({});let r={};return Le(i).pipe($t(o=>ak(t[o],n,e).pipe(pi(),Ze(s=>{if(s instanceof _s)throw Ru(new sr,s);r[o]=s}))),Il(1),ne(()=>r),Or(o=>oE(o)?Ye:Js(o)))}function ak(t,n,e){let i=n._environmentInjector,r=Es(t,i),o=r.resolve?r.resolve(n,e):ut(i,()=>r(n,e));return fo(o)}function NC(t){return We(n=>{let e=t(n);return e?Le(e).pipe(ne(()=>n)):L(n)})}var Mg=(()=>{class t{buildTitle(e){let i,r=e.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===X);return i}getResolvedTitleForRoute(e){return e.data[lc]}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>u(hE)})}return t})(),hE=(()=>{class t extends Mg{title;constructor(e){super(),this.title=e}updateTitle(e){let i=this.buildTitle(e);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||t)(M(DC))};static \u0275prov=V({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),ws=new y("",{factory:()=>({})}),fc=new y(""),pE=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=u(Tm);loadComponent(e,i){return De(this,null,function*(){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=De(this,null,function*(){try{let o=yield PC(ut(e,()=>i.loadComponent())),s=yield gE(Vm(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}});return this.componentLoaders.set(i,r),r})}loadChildren(e,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=De(this,null,function*(){try{let o=yield mE(i,this.compiler,e,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}});return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function mE(t,n,e,i){return De(this,null,function*(){let r=yield PC(ut(e,()=>t.loadChildren())),o=yield gE(Vm(r)),s;o instanceof jd||Array.isArray(o)?s=o:s=yield n.compileModuleAsync(o),i&&i(t);let a,c,l=!1,d;return Array.isArray(s)?(c=s,l=!0):(a=s.create(e).injector,d=s,c=a.get(fc,[],{optional:!0,self:!0}).flat()),{routes:c.map(Sg),injector:a,factory:d}})}function gE(t){return De(this,null,function*(){return t})}var Pu=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>u(ck)})}return t})(),ck=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,i){return e}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),vE=new y("");var _E=new y(""),lk=()=>{},yE=new y(""),bE=(()=>{class t{currentNavigation=ee(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=ee(null);events=new S;transitionAbortWithErrorSubject=new S;configLoader=u(pE);environmentInjector=u(xe);destroyRef=u(Et);urlSerializer=u(bs);rootContexts=u(Cs);location=u(rr);inputBindingEnabled=u(Ou,{optional:!0})!==null;titleStrategy=u(Mg);options=u(ws,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||pN;urlHandlingStrategy=u(Pu);createViewTransition=u(vE,{optional:!0});navigationErrorHandler=u(yE,{optional:!0});activatedRouteInjectorFeature=u(_E,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>L(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=r=>this.events.next(new wu(r)),i=r=>this.events.next(new Du(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let i=++this.navigationId;ge(()=>{this.transitions?.next(H(C({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new st(null),this.transitions.pipe(Se(i=>i!==null),We(i=>{let r=!0,o=!1,s=new AbortController,a=()=>!o&&this.currentTransition?.id===i.id;return L(i).pipe(We(c=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",kt.SupersededByNewNavigation),Ye;this.currentTransition=i;let l=this.lastSuccessfulNavigation();this.currentNavigation.set({id:c.id,initialUrl:c.rawUrl,extractedUrl:c.extractedUrl,targetBrowserUrl:typeof c.extras.browserUrl=="string"?this.urlSerializer.parse(c.extras.browserUrl):c.extras.browserUrl,trigger:c.source,extras:c.extras,previousNavigation:l?H(C({},l),{previousNavigation:null}):null,abort:()=>s.abort(),routesRecognizeHandler:c.routesRecognizeHandler,beforeActivateHandler:c.beforeActivateHandler});let d=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),f=c.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!d&&f!=="reload")return this.events.next(new ki(c.id,this.urlSerializer.serialize(c.rawUrl),"",nc.IgnoredSameUrlNavigation)),c.resolve(!1),Ye;if(this.urlHandlingStrategy.shouldProcessUrl(c.rawUrl))return L(c).pipe(We(m=>(this.events.next(new lo(m.id,this.urlSerializer.serialize(m.extractedUrl),m.source,m.restoredState)),m.id!==this.navigationId?Ye:Promise.resolve(m))),ik(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,s.signal),Ze(m=>{i.targetSnapshot=m.targetSnapshot,i.urlAfterRedirects=m.urlAfterRedirects,this.currentNavigation.update(g=>(g.finalUrl=m.urlAfterRedirects,g)),this.events.next(new rc)}),We(m=>Le(i.routesRecognizeHandler.deferredHandle??L(void 0)).pipe(ne(()=>m))),Ze(()=>{let m=new ic(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(m)}));if(d&&this.urlHandlingStrategy.shouldProcessUrl(c.currentRawUrl)){let{id:m,extractedUrl:g,source:b,restoredState:I,extras:N}=c,ie=new lo(m,this.urlSerializer.serialize(g),b,I);this.events.next(ie);let ot=XC(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=H(C({},c),{targetSnapshot:ot,urlAfterRedirects:g,extras:H(C({},N),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(Mt=>(Mt.finalUrl=g,Mt)),L(i)}else return this.events.next(new ki(c.id,this.urlSerializer.serialize(c.extractedUrl),"",nc.IgnoredByUrlHandlingStrategy)),c.resolve(!1),Ye}),ne(c=>{let l=new yu(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);return this.events.next(l),this.currentTransition=i=H(C({},c),{guards:yN(c.targetSnapshot,c.currentSnapshot,this.rootContexts)}),i}),AN(c=>this.events.next(c)),We(c=>{if(i.guardsResult=c.guardsResult,c.guardsResult&&typeof c.guardsResult!="boolean")throw Ru(this.urlSerializer,c.guardsResult);let l=new bu(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot,!!c.guardsResult);if(this.events.next(l),!a())return Ye;if(!c.guardsResult)return this.cancelNavigationTransition(c,"",kt.GuardRejected),Ye;if(c.guards.canActivateChecks.length===0)return L(c);let d=new Cu(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);if(this.events.next(d),!a())return Ye;let f=!1;return L(c).pipe(rk(this.paramsInheritanceStrategy),Ze({next:()=>{f=!0;let m=new Eu(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects),c.targetSnapshot);this.events.next(m)},complete:()=>{f||this.cancelNavigationTransition(c,"",kt.NoDataFromResolver)}}))}),NC(c=>{let l=f=>{let m=[];if(f.routeConfig?._loadedComponent)f.component=f.routeConfig?._loadedComponent;else if(f.routeConfig?.loadComponent){let g=f._environmentInjector;m.push(this.configLoader.loadComponent(g,f.routeConfig).then(b=>{f.component=b}))}for(let g of f.children)m.push(...l(g));return m},d=l(c.targetSnapshot.root);return d.length===0?L(c):Le(Promise.all(d).then(()=>c))}),We(c=>{let{newlyCreatedRoutes:l,state:d}=mN(e.routeReuseStrategy,c.targetSnapshot,c.currentRouterState);return this.currentTransition=i=c=H(C({},c),{targetRouterState:d,newlyCreatedRoutes:l}),this.currentNavigation.update(f=>(f.targetRouterState=d,f)),L(c)}),this.activatedRouteInjectorFeature?.operator()??(c=>c),NC(()=>this.afterPreactivation()),We(()=>{let{currentSnapshot:c,targetSnapshot:l}=i,d=this.createViewTransition?.(this.environmentInjector,c.root,l.root);return d?Le(d).pipe(ne(()=>i)):L(i)}),Gt(1),We(c=>{r=!1,this.events.next(new ms);let l=i.beforeActivateHandler.deferredHandle;return l?Le(l.then(()=>c)):L(c)}),Ze(c=>{new yg(e.routeReuseStrategy,i.targetRouterState,i.currentRouterState,l=>this.events.next(l),this.inputBindingEnabled).activate(this.rootContexts),c.newlyCreatedRoutes?.clear(),a()&&(o=!0,this.currentNavigation.update(l=>(l.abort=lk,l)),this.lastSuccessfulNavigation.set(ge(this.currentNavigation)),this.events.next(new ai(c.id,this.urlSerializer.serialize(c.extractedUrl),this.urlSerializer.serialize(c.urlAfterRedirects))),this.titleStrategy?.updateTitle(c.targetRouterState.snapshot),c.resolve(!0))}),ke(sE(s.signal).pipe(Se(()=>!o&&r),Ze(()=>{this.cancelNavigationTransition(i,s.signal.reason+"",kt.Aborted)}))),Ze({complete:()=>{o=!0}}),ke(this.transitionAbortWithErrorSubject.pipe(Ze(c=>{throw c}))),Fr(()=>{s.abort(),o||this.cancelNavigationTransition(i,"",kt.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),Or(c=>{if(o=!0,kC(i),this.destroyed)return i.resolve(!1),Ye;if(rE(c))this.events.next(new Mn(i.id,this.urlSerializer.serialize(i.extractedUrl),c.message,c.cancellationCode)),_N(c)?this.events.next(new gs(c.url,c.navigationBehaviorOptions)):i.resolve(!1);else{let l=new uo(i.id,this.urlSerializer.serialize(i.extractedUrl),c,i.targetSnapshot??void 0);try{let d=ut(this.environmentInjector,()=>this.navigationErrorHandler?.(l));if(d instanceof _s){let{message:f,cancellationCode:m}=Ru(this.urlSerializer,d);this.events.next(new Mn(i.id,this.urlSerializer.serialize(i.extractedUrl),f,m)),this.events.next(new gs(d.redirectTo,d.navigationBehaviorOptions))}else throw this.events.next(l),c}catch(d){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(d)}}return Ye}))}))}cancelNavigationTransition(e,i,r){kC(e);let o=new Mn(e.id,this.urlSerializer.serialize(e.extractedUrl),i,r);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=ge(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return e.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function dk(t){return t!==Ja}function kC(t){if(t.newlyCreatedRoutes)for(let n of t.newlyCreatedRoutes)n._localInjector?.destroy()}var CE=new y("");var EE=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>u(uk)})}return t})(),ku=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},uk=(()=>{class t extends ku{static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),Lu=(()=>{class t{urlSerializer=u(bs);options=u(ws,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=u(rr);urlHandlingStrategy=u(Pu);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new ln;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:i,targetBrowserUrl:r}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,i):i,s=r??o;return s instanceof ln?this.urlSerializer.serialize(s):s}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:i,initialUrl:r}){i&&e?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=e):this.rawUrlTree=r}routerState=XC(null,u(xe));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:()=>u(fk)})}return t})(),fk=(()=>{class t extends Lu{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{e(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(e,i){e instanceof lo?this.updateStateMemento():e instanceof ki?this.commitTransition(i):e instanceof ic?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof ms?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):e instanceof Mn&&!QC(e)?this.restoreHistory(i):e instanceof uo?this.restoreHistory(i,!0):e instanceof ai&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,i){let{extras:r,id:o}=i,{replaceUrl:s,state:a}=r;if(this.location.isCurrentPathEqualTo(e)||s){let c=this.browserPageId,l=C(C({},a),this.generateNgRouterState(o,c,i));this.location.replaceState(e,"",l)}else{let c=C(C({},a),this.generateNgRouterState(o,this.browserPageId+1,i));this.location.go(e,"",c)}}restoreHistory(e,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,i,r){return this.canceledNavigationResolution==="computed"?C({navigationId:e,\u0275routerPageId:i},this.routerUrlState(r)):C({navigationId:e},this.routerUrlState(r))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function Tg(t,n){t.events.pipe(Se(e=>e instanceof ai||e instanceof Mn||e instanceof uo||e instanceof ki),ne(e=>e instanceof ai||e instanceof ki?0:(e instanceof Mn?e.code===kt.Redirect||e.code===kt.SupersededByNewNavigation:!1)?2:1),Se(e=>e!==2),Gt(1)).subscribe(()=>{n()})}var Ve=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=u(Hd);stateManager=u(Lu);options=u(ws,{optional:!0})||{};pendingTasks=u(wi);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=u(bE);urlSerializer=u(bs);location=u(rr);urlHandlingStrategy=u(Pu);injector=u(xe);_events=new S;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=u(EE);injectorCleanup=u(CE,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=u(fc,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!u(Ou,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new fe;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=ge(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof Mn&&i.code!==kt.Redirect&&i.code!==kt.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof ai)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof gs){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=C({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||dk(r.source)},s);this.scheduleNavigation(a,Ja,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}fN(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),Ja,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,i,r,o)=>{this.navigateToSyncWithBrowser(e,r,i,o)})}navigateToSyncWithBrowser(e,i,r,o){let s=r?.navigationId?r:null,a=r?.\u0275routerUrl??e;if(r?.\u0275routerUrl&&(o=H(C({},o),{browserUrl:e})),r){let l=C({},r);delete l.navigationId,delete l.\u0275routerPageId,delete l.\u0275routerUrl,Object.keys(l).length!==0&&(o.state=l)}let c=this.parseUrl(a);this.scheduleNavigation(c,i,s,o).catch(l=>{this.disposed||this.injector.get(sn)(l)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return ge(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(Sg),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:s,d=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":d=C(C({},this.currentUrlTree.queryParams),o);break;case"preserve":d=this.currentUrlTree.queryParams;break;default:d=o||null}d!==null&&(d=this.removeEmptyProps(d));let f;try{let m=r?r.snapshot:this.routerState.snapshot.root;f=qC(m)}catch(m){(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),f=this.currentUrlTree.root}return YC(f,e,d,l??null,this.urlSerializer)}navigateByUrl(e,i={skipLocationChange:!1}){let r=ar(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,Ja,null,i)}navigate(e,i={skipLocationChange:!1}){return hk(e),this.navigateByUrl(this.createUrlTree(e,i),i)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch(i){return this.console.warn(vi(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,i){let r;if(i===!0?r=C({},VC):i===!1?r=C({},ug):r=C(C({},ug),i),ar(e))return IC(this.currentUrlTree,e,r);let o=this.parseUrl(e);return IC(this.currentUrlTree,o,r)}removeEmptyProps(e){return Object.entries(e).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(e,i,r,o,s){if(this.disposed)return Promise.resolve(!1);let a,c,l;s?(a=s.resolve,c=s.reject,l=s.promise):l=new Promise((f,m)=>{a=f,c=m});let d=this.pendingTasks.add();return Tg(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(d))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function hk(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new D(4008,!1)}var gk=(()=>{class t{router=u(Ve);stateManager=u(Lu);fragment=ee("");queryParams=ee({});path=ee("");serializer=u(bs);constructor(){this.updateState(),this.router.events?.subscribe(e=>{e instanceof ai&&this.updateState()})}updateState(){let{fragment:e,root:i,queryParams:r}=this.stateManager.getCurrentUrlTree();this.fragment.set(e),this.queryParams.set(r),this.path.set(this.serializer.serialize(new ln(i)))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),rt=(()=>{class t{router;route;tabIndexAttribute;renderer;el;locationStrategy;hrefAttributeValue=u(new nr("href"),{optional:!0});reactiveHref=Am(()=>this.isAnchorElement?this.computeHref(this._urlTree()):this.hrefAttributeValue);get href(){return ge(this.reactiveHref)}set href(e){this.reactiveHref.set(e)}set target(e){this._target.set(e)}get target(){return ge(this._target)}_target=ee(void 0);set queryParams(e){this._queryParams.set(e)}get queryParams(){return ge(this._queryParams)}_queryParams=ee(void 0,{equal:()=>!1});set fragment(e){this._fragment.set(e)}get fragment(){return ge(this._fragment)}_fragment=ee(void 0);set queryParamsHandling(e){this._queryParamsHandling.set(e)}get queryParamsHandling(){return ge(this._queryParamsHandling)}_queryParamsHandling=ee(void 0);set state(e){this._state.set(e)}get state(){return ge(this._state)}_state=ee(void 0,{equal:()=>!1});set info(e){this._info.set(e)}get info(){return ge(this._info)}_info=ee(void 0,{equal:()=>!1});set relativeTo(e){this._relativeTo.set(e)}get relativeTo(){return ge(this._relativeTo)}_relativeTo=ee(void 0);set preserveFragment(e){this._preserveFragment.set(e)}get preserveFragment(){return ge(this._preserveFragment)}_preserveFragment=ee(!1);set skipLocationChange(e){this._skipLocationChange.set(e)}get skipLocationChange(){return ge(this._skipLocationChange)}_skipLocationChange=ee(!1);set replaceUrl(e){this._replaceUrl.set(e)}get replaceUrl(){return ge(this._replaceUrl)}_replaceUrl=ee(!1);browserUrl=ss(void 0);isAnchorElement;onChanges=new S;applicationErrorHandler=u(sn);options=u(ws,{optional:!0});reactiveRouterState=u(gk);constructor(e,i,r,o,s,a){this.router=e,this.route=i,this.tabIndexAttribute=r,this.renderer=o,this.el=s,this.locationStrategy=a;let c=s.nativeElement.tagName?.toLowerCase();this.isAnchorElement=c==="a"||c==="area"||!!(typeof customElements=="object"&&customElements.get(c)?.observedAttributes?.includes?.("href"))}setTabIndexIfNotOnNativeEl(e){this.tabIndexAttribute!=null||this.isAnchorElement||this.applyAttributeValue("tabindex",e)}ngOnChanges(e){this.onChanges.next(this)}routerLinkInput=ee(null);set routerLink(e){e==null?(this.routerLinkInput.set(null),this.setTabIndexIfNotOnNativeEl(null)):(ar(e)?this.routerLinkInput.set(e):this.routerLinkInput.set(Array.isArray(e)?e:[e]),this.setTabIndexIfNotOnNativeEl("0"))}onClick(e,i,r,o,s){let a=this._urlTree();if(a===null||this.isAnchorElement&&(e!==0||i||r||o||s||typeof this.target=="string"&&this.target!="_self"))return!0;let c=this.browserUrl(),l=C({skipLocationChange:this.skipLocationChange,replaceUrl:this.replaceUrl,state:this.state,info:this.info},c!==void 0&&{browserUrl:c});return this.router.navigateByUrl(a,l)?.catch(d=>{this.applicationErrorHandler(d)}),!this.isAnchorElement}ngOnDestroy(){}applyAttributeValue(e,i){let r=this.renderer,o=this.el.nativeElement;i!==null?r.setAttribute(o,e,i):r.removeAttribute(o,e)}_urlTree=qt(()=>{this.reactiveRouterState.path(),this._preserveFragment()&&this.reactiveRouterState.fragment();let e=r=>r==="preserve"||r==="merge";(e(this._queryParamsHandling())||e(this.options?.defaultQueryParamsHandling))&&this.reactiveRouterState.queryParams();let i=this.routerLinkInput();return i===null||!this.router.createUrlTree?null:ar(i)?i:this.router.createUrlTree(i,{relativeTo:this._relativeTo()!==void 0?this._relativeTo():this.route,queryParams:this._queryParams(),fragment:this._fragment(),queryParamsHandling:this._queryParamsHandling(),preserveFragment:this._preserveFragment()})},{equal:(e,i)=>this.computeHref(e)===this.computeHref(i)});get urlTree(){return ge(this._urlTree)}computeHref(e){return e!==null&&this.locationStrategy?this.locationStrategy?.prepareExternalUrl(this.router.serializeUrl(e))??"":null}static \u0275fac=function(i){return new(i||t)(x(Ve),x(Yt),Ta("tabindex"),x(je),x(j),x(cs))};static \u0275dir=k({type:t,selectors:[["","routerLink",""]],hostVars:2,hostBindings:function(i,r){i&1&&F("click",function(s){return r.onClick(s.button,s.ctrlKey,s.shiftKey,s.altKey,s.metaKey)}),i&2&&he("href",r.reactiveHref(),Yp)("target",r._target())},inputs:{target:"target",queryParams:"queryParams",fragment:"fragment",queryParamsHandling:"queryParamsHandling",state:"state",info:"info",relativeTo:"relativeTo",preserveFragment:[2,"preserveFragment","preserveFragment",B],skipLocationChange:[2,"skipLocationChange","skipLocationChange",B],replaceUrl:[2,"replaceUrl","replaceUrl",B],browserUrl:[1,"browserUrl"],routerLink:"routerLink"},features:[tt]})}return t})();var vk=new y("");function Ag(t,...n){return _i([{provide:fc,multi:!0,useValue:t},{provide:Yt,useFactory:_k},{provide:Fa,multi:!0,useFactory:yk},n.map(e=>e.\u0275providers)])}function _k(){return u(Ve).routerState.root}function yk(){let t=u(ae);return n=>{let e=t.get(en);if(n!==e.components[0])return;let i=t.get(Ve),r=t.get(bk);t.get(Ck)===1&&i.initialNavigation(),t.get(Ek,null,{optional:!0})?.setUpPreloading(),t.get(vk,null,{optional:!0})?.init(),i.resetRootComponentType(e.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var bk=new y("",{factory:()=>new S}),Ck=new y("",{factory:()=>1});var Ek=new y("");var RE=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||t)(x(je),x(j))};static \u0275dir=k({type:t})}return t})(),wk=(()=>{class t extends RE{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Gn(t)))(r||t)}})();static \u0275dir=k({type:t,features:[Be]})}return t})(),NE=new y("");var Dk={provide:NE,useExisting:hn(()=>pt),multi:!0};function xk(){let t=In()?In().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var Ik=new y(""),pt=(()=>{class t extends RE{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!xk())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||t)(x(je),x(j),x(Ik,8))};static \u0275dir=k({type:t,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&F("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[ze([Dk]),Be]})}return t})();function kg(t){return t==null||Og(t)===0}function Og(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var Zu=new y(""),Fg=new y(""),Sk=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,po=class{static min(n){return Mk(n)}static max(n){return Tk(n)}static required(n){return kE(n)}static requiredTrue(n){return Ak(n)}static email(n){return Rk(n)}static minLength(n){return Nk(n)}static maxLength(n){return kk(n)}static pattern(n){return Ok(n)}static nullValidator(n){return ju()}static compose(n){return jE(n)}static composeAsync(n){return BE(n)}};function Mk(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function Tk(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function kE(t){return kg(t.value)?{required:!0}:null}function Ak(t){return t.value===!0?null:{required:!0}}function Rk(t){return kg(t.value)||Sk.test(t.value)?null:{email:!0}}function Nk(t){return n=>{let e=n.value?.length??Og(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function kk(t){return n=>{let e=n.value?.length??Og(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function Ok(t){if(!t)return ju;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),i=>{if(kg(i.value))return null;let r=i.value;return n.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function ju(t){return null}function OE(t){return t!=null}function FE(t){return tr(t)?Le(t):t}function PE(t){let n={};return t.forEach(e=>{n=e!=null?C(C({},n),e):n}),Object.keys(n).length===0?null:n}function LE(t,n){return n.map(e=>e(t))}function Fk(t){return!t.validate}function VE(t){return t.map(n=>Fk(n)?n:e=>n.validate(e))}function jE(t){if(!t)return null;let n=t.filter(OE);return n.length==0?null:function(e){return PE(LE(e,n))}}function Pg(t){return t!=null?jE(VE(t)):null}function BE(t){if(!t)return null;let n=t.filter(OE);return n.length==0?null:function(e){let i=LE(e,n).map(FE);return ta(i).pipe(ne(PE))}}function Lg(t){return t!=null?BE(VE(t)):null}function wE(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function UE(t){return t._rawValidators}function HE(t){return t._rawAsyncValidators}function Rg(t){return t?Array.isArray(t)?t:[t]:[]}function Bu(t,n){return Array.isArray(t)?t.includes(n):t===n}function DE(t,n){let e=Rg(n);return Rg(t).forEach(r=>{Bu(e,r)||e.push(r)}),e}function xE(t,n){return Rg(n).filter(e=>!Bu(t,e))}var Uu=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=Pg(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=Lg(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},cr=class extends Uu{name;get formDirective(){return null}get path(){return null}};var hc="VALID",Vu="INVALID",Ds="PENDING",pc="DISABLED",lr=class{},Hu=class extends lr{value;source;constructor(n,e){super(),this.value=n,this.source=e}},gc=class extends lr{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},vc=class extends lr{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},xs=class extends lr{status;source;constructor(n,e){super(),this.status=n,this.source=e}},zu=class extends lr{source;constructor(n){super(),this.source=n}},Is=class extends lr{source;constructor(n){super(),this.source=n}};function zE(t){return(Ku(t)?t.validators:t)||null}function Pk(t){return Array.isArray(t)?Pg(t):t||null}function $E(t,n){return(Ku(n)?n.asyncValidators:t)||null}function Lk(t){return Array.isArray(t)?Lg(t):t||null}function Ku(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function Vk(t,n,e){let i=t.controls;if(!(n?Object.keys(i):i).length)throw new D(1e3,"");if(!GE(i,e))throw new D(1001,"")}function jk(t,n,e){t._forEachChild((i,r)=>{if(e[r]===void 0)throw new D(-1002,"")})}var $u=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=ee(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return ge(this.statusReactive)}set status(n){ge(()=>this.statusReactive.set(n))}_status=qt(()=>this.statusReactive());statusReactive=ee(void 0);get valid(){return this.status===hc}get invalid(){return this.status===Vu}get pending(){return this.status===Ds}get disabled(){return this.status===pc}get enabled(){return this.status!==pc}errors;get pristine(){return ge(this.pristineReactive)}set pristine(n){ge(()=>this.pristineReactive.set(n))}_pristine=qt(()=>this.pristineReactive());pristineReactive=ee(!0);get dirty(){return!this.pristine}get touched(){return ge(this.touchedReactive)}set touched(n){ge(()=>this.touchedReactive.set(n))}_touched=qt(()=>this.touchedReactive());touchedReactive=ee(!1);get untouched(){return!this.touched}_events=new S;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(DE(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(DE(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(xE(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(xE(n,this._rawAsyncValidators))}hasValidator(n){return Bu(this._rawValidators,n)}hasAsyncValidator(n){return Bu(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(H(C({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new vc(!0,i))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:i})}),n.onlySelf||this._parent?._updateTouched(n,i),e&&n.emitEvent!==!1&&this._events.next(new vc(!1,i))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let i=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(H(C({},n),{sourceControl:i})),e&&n.emitEvent!==!1&&this._events.next(new gc(!1,i))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=n.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,i),e&&n.emitEvent!==!1&&this._events.next(new gc(!0,i))}markAsPending(n={}){this.status=Ds;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new xs(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(H(C({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=pc,this.errors=null,this._forEachChild(r=>{r.disable(H(C({},n),{onlySelf:!0}))}),this._updateValue();let i=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Hu(this.value,i)),this._events.next(new xs(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(H(C({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=hc,this._forEachChild(i=>{i.enable(H(C({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(H(C({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===hc||this.status===Ds)&&this._runAsyncValidator(i,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Hu(this.value,e)),this._events.next(new xs(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(H(C({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?pc:hc}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=Ds,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let i=FE(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(n,e){let i=e?this.get(e):this;return i?.errors?i.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,i){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||i)&&this._events.next(new xs(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,i)}_initObservables(){this.valueChanges=new Y,this.statusChanges=new Y}_calculateStatus(){return this._allControlsDisabled()?pc:this.errors?Vu:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Ds)?Ds:this._anyControlsHaveStatus(Vu)?Vu:hc}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,n.onlySelf||this._parent?._updatePristine(n,e),r&&this._events.next(new gc(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new vc(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Ku(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=Pk(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=Lk(this._rawAsyncValidators)}_updateHasRequiredValidator(){ge(()=>this._hasRequired.set(this.hasValidator(po.required)))}};function GE(t,n){return Object.hasOwn(t,n)}function Bk(t){return t.tagName==="INPUT"||t.tagName==="SELECT"||t.tagName==="TEXTAREA"}function Uk(t,n,e,i){switch(e){case"name":t.setAttribute(n,e,i);break;case"disabled":case"readonly":case"required":i?t.setAttribute(n,e,""):t.removeAttribute(n,e);break;case"max":case"min":case"minLength":case"maxLength":i!==void 0?t.setAttribute(n,e,i.toString()):t.removeAttribute(n,e);break}}var Ng=class{kind;context;control;message;constructor({kind:n,context:e,control:i}){this.kind=n,this.context=e,this.control=i}};var Hk=(()=>{class t{_validator=ju;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let i=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(i),this._validator=this._enabled?this.createValidator(i):ju,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,features:[tt]})}return t})();var zk={provide:Zu,useExisting:hn(()=>Tn),multi:!0};var Tn=(()=>{class t extends Hk{required;inputName="required";normalizeInput=B;createValidator=e=>kE;enabled(e){return e}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Gn(t)))(r||t)}})();static \u0275dir=k({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(i,r){i&2&&he("required",r._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[ze([zk]),Be]})}return t})();var $k=new y(""),Qu=new y("",{factory:()=>Vg}),Vg="always";function Gk(t,n){return[...n.path,t]}function IE(t,n,e=Vg){jg(t,n),n.valueAccessor.writeValue(t.value),(t.disabled||e==="always")&&n.valueAccessor.setDisabledState?.(t.disabled),qk(t,n),Zk(t,n),Yk(t,n),Wk(t,n)}function SE(t,n,e=!0){let i=()=>{};n?.valueAccessor?.registerOnChange(i),n?.valueAccessor?.registerOnTouched(i),Wu(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function Gu(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function Wk(t,n){if(n.valueAccessor.setDisabledState){let e=i=>{n.valueAccessor.setDisabledState(i)};t.registerOnDisabledChange(e),n._registerOnDestroy(()=>{t._unregisterOnDisabledChange(e)})}}function jg(t,n){let e=UE(t);n.validator!==null?t.setValidators(wE(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let i=HE(t);n.asyncValidator!==null?t.setAsyncValidators(wE(i,n.asyncValidator)):typeof i=="function"&&t.setAsyncValidators([i]);let r=()=>t.updateValueAndValidity();Gu(n._rawValidators,r),Gu(n._rawAsyncValidators,r)}function Wu(t,n){let e=!1;if(t!==null){if(n.validator!==null){let r=UE(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.validator);o.length!==r.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let r=HE(t);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==n.asyncValidator);o.length!==r.length&&(e=!0,t.setAsyncValidators(o))}}}let i=()=>{};return Gu(n._rawValidators,i),Gu(n._rawAsyncValidators,i),e}function qk(t,n){n.valueAccessor.registerOnChange(e=>{t._pendingValue=e,t._pendingChange=!0,t._pendingDirty=!0,t.updateOn==="change"&&WE(t,n)})}function Yk(t,n){n.valueAccessor.registerOnTouched(()=>{t._pendingTouched=!0,t.updateOn==="blur"&&t._pendingChange&&WE(t,n),t.updateOn!=="submit"&&t.markAsTouched()})}function WE(t,n){t._pendingDirty&&t.markAsDirty(),t.setValue(t._pendingValue,{emitModelToViewChange:!1}),n.viewToModelUpdate(t._pendingValue),t._pendingChange=!1}function Zk(t,n){let e=(i,r)=>{n.valueAccessor.writeValue(i),r&&n.viewToModelUpdate(i)};t.registerOnChange(e),n._registerOnDestroy(()=>{t._unregisterOnChange(e)})}function qE(t,n){t==null,jg(t,n)}function Kk(t,n){return Wu(t,n)}function Qk(t,n){if(!t.hasOwnProperty("model"))return!1;let e=t.model;return e.isFirstChange()?!0:!Object.is(n,e.currentValue)}function Xk(t){return Object.getPrototypeOf(t.constructor)===wk}function YE(t,n){t._syncPendingControls(),n.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function Jk(t,n){if(!n)return null;Array.isArray(n);let e,i,r;return n.forEach(o=>{o.constructor===pt?e=o:Xk(o)?i=o:r=o}),r||i||e||null}function eO(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var tO={provide:$k,useFactory:()=>{let t=u(ci,{self:!0});return{setParseErrors:n=>{t.setParseErrorSource(n)},set onReset(n){t.onReset=n}}}},ci=class extends Uu{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(n){this.userOnReset=n,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(e=>{e instanceof Is&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=Jk(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;constructor(n,e,i){super(),this.injector=n,this.renderer=e,this.rawValueAccessors=i,this.injector?.get(Et)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let n=this.injector?.get(me);if(!this.control||!n)return;let e=n.markForCheck.bind(n);this.subscription=new fe,this.subscription.add(this.control.valueChanges.subscribe(e)),this.subscription.add(this.control.statusChanges.subscribe(e)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=this.control.events.subscribe(i=>{i instanceof Is&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&this.control.addValidators(this.parseErrorsValidator)}ngControlCreate(n){!n.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!n.customControl||(this.isCustomControlBased=!0,n.listenToCustomControlModel(r=>{this.control?.setValue(r,{emitModelToViewChange:!1}),this.control?.markAsDirty(),this.viewToModelUpdate(r)}),n.listenToCustomControlOutput("touch",()=>{this.control?.markAsTouched()}),this.customControlBindings={},this.isNativeFormElement=Bk(n.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(r=>r instanceof Tn))}ngControlUpdate(n,e){if(!this.isCustomControlBased)return;let i=this.control,r=this.customControlBindings;Object.is(r.value,i.value)||(r.value=i.value,n.setCustomControlModelInput(i.value)),this.bindControlProperty(n,r,"touched",i.touched),this.bindControlProperty(n,r,"dirty",i.dirty),this.bindControlProperty(n,r,"valid",i.valid),this.bindControlProperty(n,r,"invalid",i.invalid),this.bindControlProperty(n,r,"pending",i.pending),this.bindControlProperty(n,r,"disabled",i.disabled),this.shouldBindRequired&&this.bindControlProperty(n,r,"required",this.isRequired);let o=i.errors;if(r.errors!==o){r.errors=o;let s=this._convertErrors(o);n.setInputOnDirectives("errors",s)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(n,e,i,r){if(e[i]===r)return;e[i]=r;let o=n.setInputOnDirectives(i,r);this.isNativeFormElement&&!o&&(i==="disabled"||i==="required")&&this.renderer&&Uk(this.renderer,n.nativeElement,i,r)}_convertErrors(n){if(n===null)return[];let e=this.control;return Object.entries(n).map(([i,r])=>new Ng({context:r,kind:i,control:e}))}setParseErrorSource(n){if(n===void 0)return;let e=null,i=qt(()=>{let r=n();return r.length===0?null:r.reduce((o,s)=>(o[s.kind]=s,o),{})});this.parseErrorsValidator=(()=>e).bind(this),Bn(()=>{e=i(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(n){this.parseErrorsValidator&&(n?.removeValidators(this.parseErrorsValidator),n?.updateValueAndValidity({emitEvent:!1}))}},qu=class{_cd;constructor(n){this._cd=n}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var Ot=(()=>{class t extends qu{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(x(ci,2))};static \u0275dir=k({type:t,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&ce("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[Be]})}return t})(),li=(()=>{class t extends qu{constructor(e){super(e)}static \u0275fac=function(i){return new(i||t)(x(cr,10))};static \u0275dir=k({type:t,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&ce("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[Be]})}return t})(),Yu=class extends $u{constructor(n,e,i){super(zE(e),$E(i,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){let i=this._find(n);return i||(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,i={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){let i=this._find(n);i&&i._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,i={}){let r=this._find(n);r&&r._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(n){return this._find(n)?.enabled===!0}setValue(n,e={}){ge(()=>{jk(this,!0,n),Object.keys(n).forEach(i=>{Vk(this,!0,i),this.controls[i].setValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)})}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(i=>{let r=this._find(i);r&&r.patchValue(n[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((i,r)=>{i.reset(n?n[r]:null,H(C({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Is(this))}getRawValue(){return this._reduceChildren({},(n,e,i)=>(n[i]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&n(i,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&n(i))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(n,e){let i=n;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return GE(this.controls,n)?this.controls[n]:null}};var nO={provide:cr,useExisting:hn(()=>Zt)},mc=Promise.resolve(),Zt=(()=>{class t extends cr{callSetDisabledState;get submitted(){return ge(this.submittedReactive)}_submitted=qt(()=>this.submittedReactive());submittedReactive=ee(!1);_directives=new Set;form;ngSubmit=new Y;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new Yu({},Pg(e),Lg(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){mc.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),e._setupWithForm(this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){mc.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){mc.then(()=>{let i=this._findContainer(e.path),r=new Yu({});qE(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){mc.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){mc.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),YE(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new zu(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||t)(x(Zu,10),x(Fg,10),x(Qu,8))};static \u0275dir=k({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&F("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[ze([nO]),Be]})}return t})();function ME(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function TE(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var ZE=class extends $u{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,i){super(zE(e),$E(i,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Ku(e)&&(e.nonNullable||e.initialValueIsDefault)&&(TE(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){ge(()=>{this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)})}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new Is(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){ME(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){ME(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){TE(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var iO=t=>t instanceof ZE;var rO={provide:ci,useExisting:hn(()=>Dt)},AE=Promise.resolve(),Dt=(()=>{class t extends ci{_changeDetectorRef;callSetDisabledState;control=new ZE;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new Y;constructor(e,i,r,o,s,a,c,l){super(c,l,o),this._changeDetectorRef=s,this.callSetDisabledState=a,this._parent=e,this._setValidators(i),this._setAsyncValidators(r)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),Qk(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}\u0275ngControlCreate(e){super.ngControlCreate(e)}\u0275ngControlUpdate(e){super.ngControlUpdate(e,!1)}get shouldBindRequired(){return!1}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,IE(this.control,this,this.callSetDisabledState)),this.control.updateValueAndValidity({emitEvent:!1})}_setupWithForm(e){this.isCustomControlBased?this.setupCustomControl():(this.valueAccessor??=this.selectedValueAccessor,IE(this.control,this,e))}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){AE.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&B(i);AE.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?Gk(e,this._parent):[e]}static \u0275fac=function(i){return new(i||t)(x(cr,9),x(Zu,10),x(Fg,10),x(NE,10),x(me,8),x(Qu,8),x(ae,8),x(je,8))};static \u0275dir=k({type:t,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[ze([rO,tO]),Be,tt,Dm(null)]})}return t})();var di=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return t})();var oO=(()=>{class t extends cr{callSetDisabledState;get submitted(){return ge(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=qt(()=>this._submittedReactive());_submittedReactive=ee(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Wu(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return e._setupWithForm(i,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){SE(e.control||null,e,!1),eO(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,YE(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new zu(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(SE(i||null,e),iO(r)&&e._setupWithForm(r,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);qE(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&Kk(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){jg(this.form,this),this._oldForm&&Wu(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||t)(x(Zu,10),x(Fg,10),x(Qu,8))};static \u0275dir=k({type:t,features:[Be,tt]})}return t})();var sO={provide:cr,useExisting:hn(()=>_c)},_c=(()=>{class t extends oO{form=null;ngSubmit=new Y;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Gn(t)))(r||t)}})();static \u0275dir=k({type:t,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&F("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[ze([sO]),Be]})}return t})();var aO=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({})}return t})();var Ft=(()=>{class t{static withConfig(e){return{ngModule:t,providers:[{provide:Qu,useValue:e.callSetDisabledState??Vg}]}}static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({imports:[aO]})}return t})();var cO=new y("cdk-dir-doc",{providedIn:"root",factory:()=>u(U)}),lO=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function KE(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?lO.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var An=(()=>{class t{get value(){return this.valueSignal()}valueSignal=ee("ltr");change=new Y;constructor(){let e=u(cO,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(KE(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Ie=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({})}return t})();var dO=["*"];var uO=new y("MAT_CARD_CONFIG"),Pt=(()=>{class t{appearance;constructor(){let e=u(uO,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,r){i&2&&ce("mat-mdc-card-outlined",r.appearance==="outlined")("mdc-card--outlined",r.appearance==="outlined")("mat-mdc-card-filled",r.appearance==="filled")("mdc-card--filled",r.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:dO,decls:1,vars:0,template:function(i,r){i&1&&(He(),ue(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-color: var(--mat-card-elevated-container-color, var(--mat-sys-surface-container-low));
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-elevated-container-elevation, var(--mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--mat-card-elevated-container-shape, var(--mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--mat-card-outlined-container-color, var(--mat-sys-surface));
  border-radius: var(--mat-card-outlined-container-shape, var(--mat-sys-corner-medium));
  border-width: var(--mat-card-outlined-outline-width, 1px);
  border-color: var(--mat-card-outlined-outline-color, var(--mat-sys-outline-variant));
  box-shadow: var(--mat-card-outlined-container-elevation, var(--mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--mat-card-filled-container-color, var(--mat-sys-surface-container-highest));
  border-radius: var(--mat-card-filled-container-shape, var(--mat-sys-corner-medium));
  box-shadow: var(--mat-card-filled-container-elevation, var(--mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--mat-card-title-text-font, var(--mat-sys-title-large-font));
  line-height: var(--mat-card-title-text-line-height, var(--mat-sys-title-large-line-height));
  font-size: var(--mat-card-title-text-size, var(--mat-sys-title-large-size));
  letter-spacing: var(--mat-card-title-text-tracking, var(--mat-sys-title-large-tracking));
  font-weight: var(--mat-card-title-text-weight, var(--mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--mat-card-subtitle-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-card-subtitle-text-font, var(--mat-sys-title-medium-font));
  line-height: var(--mat-card-subtitle-text-line-height, var(--mat-sys-title-medium-line-height));
  font-size: var(--mat-card-subtitle-text-size, var(--mat-sys-title-medium-size));
  letter-spacing: var(--mat-card-subtitle-text-tracking, var(--mat-sys-title-medium-tracking));
  font-weight: var(--mat-card-subtitle-text-weight, var(--mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2})}return t})();var Lt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({imports:[Ie]})}return t})();function mo(t){return t.buttons===0||t.detail===0}function go(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var Bg;function QE(){if(Bg==null){let t=typeof document<"u"?document.head:null;Bg=!!(t&&(t.createShadowRoot||t.attachShadow))}return Bg}function Ug(t){if(QE()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function xt(t){if(t.composedPath)try{return t.composedPath()[0]}catch(n){}return t.target}var Hg;try{Hg=typeof Intl<"u"&&Intl.v8BreakIterator}catch(t){Hg=!1}var $e=(()=>{class t{_platformId=u(Kr);isBrowser=this._platformId?rC(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||Hg)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var yc;function XE(){if(yc==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>yc=!0}))}finally{yc=yc||!1}return yc}function Ss(t){return XE()?t:!!t.capture}function un(t){return t instanceof j?t.nativeElement:t}var JE=new y("cdk-input-modality-detector-options"),ew={ignoreKeys:[18,17,224,91,16]},tw=650,zg={passive:!0,capture:!0},nw=(()=>{class t{_platform=u($e);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new st(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=xt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<tw||(this._modality.next(mo(e)?"keyboard":"mouse"),this._mostRecentTarget=xt(e))};_onTouchstart=e=>{if(go(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=xt(e)};constructor(){let e=u(O),i=u(U),r=u(JE,{optional:!0});if(this._options=C(C({},ew),r),this.modalityDetected=this._modality.pipe(Qf(1)),this.modalityChanged=this.modalityDetected.pipe(xl()),this._platform.isBrowser){let o=u(et).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,zg),o.listen(i,"mousedown",this._onMousedown,zg),o.listen(i,"touchstart",this._onTouchstart,zg)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),bc=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(bc||{}),iw=new y("cdk-focus-monitor-default-options"),Xu=Ss({passive:!0,capture:!0}),fr=(()=>{class t{_ngZone=u(O);_platform=u($e);_inputModalityDetector=u(nw);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=u(U);_stopInputModalityDetector=new S;constructor(){let e=u(iw,{optional:!0});this._detectionMode=e?.detectionMode||bc.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=xt(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=un(e);if(!this._platform.isBrowser||r.nodeType!==1)return L();let o=Ug(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new S,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let i=un(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=un(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,c])=>this._originChanged(a,i,c)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===bc.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===bc.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?tw:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=xt(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,Xu),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,Xu)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(ke(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Xu),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Xu),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Ju=new WeakMap,It=(()=>{class t{_appRef;_injector=u(ae);_environmentInjector=u(xe);load(e){let i=this._appRef=this._appRef||this._injector.get(en),r=Ju.get(i);r||(r={loaders:new Set,refs:[]},Ju.set(i,r),i.onDestroy(()=>{Ju.get(i)?.refs.forEach(o=>o.destroy()),Ju.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(Qd(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Cc=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2})}return t})(),ef;function fO(){if(ef===void 0&&(ef=null,typeof window<"u")){let t=window;t.trustedTypes!==void 0&&(ef=t.trustedTypes.createPolicy("angular#components",{createHTML:n=>n}))}return ef}function vo(t){return fO()?.createHTML(t)||t}function rw(t,n,e){let i=e.sanitize(at.HTML,n);t.innerHTML=vo(i||"")}function $g(t){return Array.isArray(t)?t:[t]}var ow=new Set,_o,Gg=(()=>{class t{_platform=u($e);_nonce=u(Qr,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):pO}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&hO(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function hO(t,n){if(!ow.has(t))try{_o||(_o=document.createElement("style"),n&&_o.setAttribute("nonce",n),_o.setAttribute("type","text/css"),document.head.appendChild(_o)),_o.sheet&&(_o.sheet.insertRule(`@media ${t.replace(/[{}]/g,"")} {body{ }}`,0),ow.add(t))}catch(e){console.error(e)}}function pO(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var mO=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var sw=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({providers:[mO]})}return t})();var aw=new y("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),cw=new y("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),gO=0,Wg=(()=>{class t{_ngZone=u(O);_defaultOptions=u(cw,{optional:!0});_liveElement;_document=u(U);_sanitizer=u(Za);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=u(aw,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,s;return i.length===1&&typeof i[0]=="number"?s=i[0]:[o,s]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),s==null&&r&&(s=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:rw(this._liveElement,e,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${gO++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var vO=200,tf=class{_letterKeyStream=new S;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new S;selectedItem=this._selectedItem;constructor(n,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:vO;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(n),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(n){this._selectedItemIndex=n}setItems(n){this._items=n}handleKey(n){let e=n.keyCode;n.key&&n.key.length===1?this._letterKeyStream.next(n.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(n){this._letterKeyStream.pipe(Ze(e=>this._pressedLetters.push(e)),Zf(n),Se(()=>this._pressedLetters.length>0),ne(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function Rn(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var Ms=class{_items;_activeItemIndex=ee(-1);_activeItem=ee(null);_wrap=!1;_typeaheadSubscription=fe.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=n=>n.disabled;constructor(n,e){this._items=n,n instanceof zn?this._itemChangesSubscription=n.changes.subscribe(i=>this._itemsChanged(i.toArray())):yn(n)&&(this._effectRef=Bn(()=>this._itemsChanged(n()),{injector:e}))}tabOut=new S;change=new S;skipPredicate(n){return this._skipPredicateFn=n,this}withWrap(n=!0){return this._wrap=n,this}withVerticalOrientation(n=!0){return this._vertical=n,this}withHorizontalOrientation(n){return this._horizontal=n,this}withAllowedModifierKeys(n){return this._allowedModifierKeys=n,this}withTypeAhead(n=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new tf(e,{debounceInterval:typeof n=="number"?n:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(n=!0){return this._homeAndEnd=n,this}withPageUpDown(n=!0,e=10){return this._pageUpAndDown={enabled:n,delta:e},this}setActiveItem(n){let e=this._activeItem();this.updateActiveItem(n),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(n){let e=n.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!n[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||Rn(n,"shiftKey"))&&this._typeahead?.handleKey(n);return}this._typeahead?.reset(),n.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(n){let e=this._getItemsArray(),i=typeof n=="number"?n:e.indexOf(n),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(n){this._wrap?this._setActiveInWrapMode(n):this._setActiveInDefaultMode(n)}_setActiveInWrapMode(n){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+n*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(n){this._setActiveItemByIndex(this._activeItemIndex()+n,n)}_setActiveItemByIndex(n,e){let i=this._getItemsArray();if(i[n]){for(;this._skipPredicateFn(i[n]);)if(n+=e,!i[n])return;this.setActiveItem(n)}}_getItemsArray(){return yn(this._items)?this._items():this._items instanceof zn?this._items.toArray():this._items}_itemsChanged(n){this._typeahead?.setItems(n);let e=this._activeItem();if(e){let i=n.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var Ic=class extends Ms{setActiveItem(n){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(n),this.activeItem&&this.activeItem.setActiveStyles()}};var yo=class extends Ms{_origin="program";setFocusOrigin(n){return this._origin=n,this}setActiveItem(n){super.setActiveItem(n),this.activeItem&&this.activeItem.focus(this._origin)}};var dw=new Map,mt=class t{_appId=u(Zr);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){this._appId!=="ng"&&(n+=this._appId);let i=dw.get(n);return i===void 0?i=0:i++,dw.set(n,i),`${n}${e?t._infix+"-":""}${i}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})};var bo;function uw(){if(bo==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return bo=!1,bo;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)bo=!0;else{let t=Element.prototype.scrollTo;t?bo=!/\{\s*\[native code\]\s*\}/.test(t.toString()):bo=!1}}return bo}function qg(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var Ts,fw=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Yg(){if(Ts)return Ts;if(typeof document!="object"||!document)return Ts=new Set(fw),Ts;let t=document.createElement("input");return Ts=new Set(fw.filter(n=>(t.setAttribute("type",n),t.type===n))),Ts}var _O=new y("MATERIAL_ANIMATIONS"),hw=null;function Zg(){return u(_O,{optional:!0})?.animationsDisabled||u(va,{optional:!0})==="NoopAnimations"?"di-disabled":(hw??=u(Gg).matchMedia("(prefers-reduced-motion)").matches,hw?"reduced-motion":"enabled")}function St(){return Zg()!=="enabled"}function Xe(t){return t==null?"":typeof t=="string"?t:`${t}px`}function As(t){return t!=null&&`${t}`!="false"}var Nn=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(Nn||{}),Kg=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=Nn.HIDDEN;constructor(n,e,i,r=!1){this._renderer=n,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},mw=Ss({passive:!0,capture:!0}),Qg=class{_events=new Map;addHandler(n,e,i,r){let o=this._events.get(e);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,mw)})}removeHandler(n,e,i){let r=this._events.get(n);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,mw)))}_delegateEventHandler=n=>{let e=xt(n);e&&this._events.get(n.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(n))})}},Sc={enterDuration:225,exitDuration:150},yO=800,gw=Ss({passive:!0,capture:!0}),vw=["mousedown","touchstart"],_w=["mouseup","mouseleave","touchend","touchcancel"],bO=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2})}return t})(),Mc=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Qg;constructor(n,e,i,r,o){this._target=n,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=un(i)),o&&o.get(It).load(bO)}fadeInRipple(n,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=C(C({},Sc),i.animation);i.centered&&(n=r.left+r.width/2,e=r.top+r.height/2);let s=i.radius||CO(n,e,r),a=n-r.left,c=e-r.top,l=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${c-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,i.color!=null&&(d.style.backgroundColor=i.color),d.style.transitionDuration=`${l}ms`,this._containerElement.appendChild(d);let f=window.getComputedStyle(d),m=f.transitionProperty,g=f.transitionDuration,b=m==="none"||g==="0s"||g==="0s, 0s"||r.width===0&&r.height===0,I=new Kg(this,d,i,b);d.style.transform="scale3d(1, 1, 1)",I.state=Nn.FADING_IN,i.persistent||(this._mostRecentTransientRipple=I);let N=null;return!b&&(l||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let ie=()=>{N&&(N.fallbackTimer=null),clearTimeout(Mt),this._finishRippleTransition(I)},ot=()=>this._destroyRipple(I),Mt=setTimeout(ot,l+100);d.addEventListener("transitionend",ie),d.addEventListener("transitioncancel",ot),N={onTransitionEnd:ie,onTransitionCancel:ot,fallbackTimer:Mt}}),this._activeRipples.set(I,N),(b||!l)&&this._finishRippleTransition(I),I}fadeOutRipple(n){if(n.state===Nn.FADING_OUT||n.state===Nn.HIDDEN)return;let e=n.element,i=C(C({},Sc),n.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",n.state=Nn.FADING_OUT,(n._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=un(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,vw.forEach(i=>{t._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{_w.forEach(e=>{this._triggerElement.addEventListener(e,this,gw)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===Nn.FADING_IN?this._startFadeOutTransition(n):n.state===Nn.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:i}=n.config;n.state=Nn.VISIBLE,!i&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=Nn.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=mo(n),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+yO;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!go(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===Nn.VISIBLE||n.config.terminateOnPointerUp&&n.state===Nn.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&(vw.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(_w.forEach(e=>n.removeEventListener(e,this,gw)),this._pointerUpEventsRegistered=!1))}};function CO(t,n,e){let i=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),r=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(i*i+r*r)}var Tc=new y("mat-ripple-global-options"),nf=(()=>{class t{_elementRef=u(j);_animationsDisabled=St();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=u(O),i=u($e),r=u(Tc,{optional:!0}),o=u(ae);this._globalOptions=r||{},this._rippleRenderer=new Mc(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:C(C(C({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,C(C({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,C(C({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&ce("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var EO={capture:!0},wO=["focus","mousedown","mouseenter","touchstart"],Xg="mat-ripple-loader-uninitialized",Jg="mat-ripple-loader-class-name",yw="mat-ripple-loader-centered",rf="mat-ripple-loader-disabled",of=(()=>{class t{_document=u(U);_animationsDisabled=St();_globalRippleOptions=u(Tc,{optional:!0});_platform=u($e);_ngZone=u(O);_injector=u(ae);_eventCleanups;_hosts=new Map;constructor(){let e=u(et).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>wO.map(i=>e.listen(this._document,i,this._onInteraction,EO)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(Xg,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(Jg))&&e.setAttribute(Jg,i.className||""),i.centered&&e.setAttribute(yw,""),i.disabled&&e.setAttribute(rf,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(rf,""):e.removeAttribute(rf)}_onInteraction=e=>{let i=xt(e);if(i instanceof HTMLElement){let r=i.closest(`[${Xg}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(Jg)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??Sc.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??Sc.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(rf),rippleConfig:{centered:e.hasAttribute(yw),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},c=new Mc(a,this._ngZone,i,this._platform,this._injector),l=!a.rippleDisabled;l&&c.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:c,hasSetUpEvents:l}),e.removeAttribute(Xg)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Oi=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2})}return t})();var DO=["*",[["","progressIndicator",""]]],xO=["*","[progressIndicator]"];function IO(t,n){t&1&&(ft(0,"div",1),ue(1,1),wt())}var SO=new y("MAT_BUTTON_CONFIG");function bw(t){return t==null?void 0:xn(t)}var ev=(()=>{class t{_elementRef=u(j);_ngZone=u(O);_animationsDisabled=St();_config=u(SO,{optional:!0});_focusMonitor=u(fr);_cleanupClick;_renderer=u(je);_rippleLoader=u(of);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}showProgress=ss(!1,{transform:B});constructor(){u(It).load(Oi);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(i,r){i&2&&(he("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),an(r.color?"mat-"+r.color:""),ce("mat-mdc-button-progress-indicator-shown",r.showProgress())("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",B],disabled:[2,"disabled","disabled",B],ariaDisabled:[2,"aria-disabled","ariaDisabled",B],disabledInteractive:[2,"disabledInteractive","disabledInteractive",B],tabIndex:[2,"tabIndex","tabIndex",bw],_tabindex:[2,"tabindex","_tabindex",bw],showProgress:[1,"showProgress"]}})}return t})(),Fi=(()=>{class t extends ev{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[Be],ngContentSelectors:xO,decls:5,vars:1,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(He(DO),En(0,"span",0),ue(1),_e(2,IO,2,0,"div",1),En(3,"span",2)(4,"span",3)),i&2&&(v(2),ye(r.showProgress()?2:-1))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--mat-icon-button-state-layer-size, 40px);
  height: var(--mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--mat-icon-button-state-layer-size, 40px) - var(--mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--mat-icon-button-icon-size, 24px);
  color: var(--mat-icon-button-icon-color, var(--mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--mat-icon-button-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface-variant) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-icon-button-touch-target-size, 48px);
  display: var(--mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--mat-icon-button-icon-size, 24px);
  height: var(--mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__determinate-circle-graphic {
  width: inherit;
  height: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__indeterminate-circle-graphic {
  height: 100%;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return t})();var hr=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({imports:[Ie]})}return t})();var MO=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]],[["","progressIndicator",""]]],TO=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]","[progressIndicator]"];function AO(t,n){t&1&&(ft(0,"div",2),ue(1,3),wt())}var Cw=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),Vt=(()=>{class t extends ev{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=RO(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?Cw.get(this._appearance):null,o=Cw.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[Be],ngContentSelectors:TO,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(He(MO),En(0,"span",0),ue(1),ft(2,"span",1),ue(3,1),wt(),ue(4,2),_e(5,AO,2,0,"div",2),En(6,"span",3)(7,"span",4)),i&2&&(ce("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab),v(5),ye(r.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button .mat-mdc-button-progress-indicator-container {
  --mat-progress-spinner-active-indicator-color: var(--mat-button-filled-progress-active-indicator-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon,
.mat-mdc-button-progress-indicator-shown [matButtonIcon],
.mat-mdc-button-progress-indicator-shown .mdc-button__label {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})}return t})();function RO(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var gt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({imports:[hr,Ie]})}return t})();var NO=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2})}return t})(),kO={passive:!0},Ew=(()=>{class t{_platform=u($e);_ngZone=u(O);_renderer=u(et).createRenderer(null,null);_styleLoader=u(It);_monitoredElements=new Map;monitor(e){if(!this._platform.isBrowser)return Ye;this._styleLoader.load(NO);let i=un(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new S,s="cdk-text-field-autofilled",a=l=>{l.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!0}))):l.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:l.target,isAutofilled:!1})))},c=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,kO)));return this._monitoredElements.set(i,{subject:o,unlisten:c}),o}stopMonitoring(e){let i=un(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var ww=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({})}return t})();var Dw=new y("MAT_INPUT_VALUE_ACCESSOR");var tv=class{_box;_destroyed=new S;_resizeSubject=new S;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new re(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),i.unsubscribe(),this._elementObservables.delete(n)}}).pipe(Se(e=>e.some(i=>i.target===n)),Ml({bufferSize:1,refCount:!0}),ke(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},xw=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=u(O);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new tv(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var OO=["notch"],FO=["*"],Iw=["iconPrefixContainer"],Sw=["textPrefixContainer"],Mw=["iconSuffixContainer"],Tw=["textSuffixContainer"],PO=["textField"],LO=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],VO=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function jO(t,n){t&1&&pe(0,"span",21)}function BO(t,n){if(t&1&&(p(0,"label",20),ue(1,1),_e(2,jO,1,0,"span",21),h()),t&2){let e=w(2);E("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),he("for",e._control.disableAutomaticLabeling?null:e._control.id),v(2),ye(!e.hideRequiredMarker&&e._control.required?2:-1)}}function UO(t,n){if(t&1&&_e(0,BO,3,5,"label",20),t&2){let e=w();ye(e._hasFloatingLabel()?0:-1)}}function HO(t,n){t&1&&pe(0,"div",7)}function zO(t,n){}function $O(t,n){if(t&1&&$(0,zO,0,0,"ng-template",13),t&2){w(2);let e=ri(1);E("ngTemplateOutlet",e)}}function GO(t,n){if(t&1&&(p(0,"div",9),_e(1,$O,1,1,null,13),h()),t&2){let e=w();E("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),v(),ye(e._forceDisplayInfixLabel()?-1:1)}}function WO(t,n){t&1&&(p(0,"div",10,2),ue(2,2),h())}function qO(t,n){t&1&&(p(0,"div",11,3),ue(2,3),h())}function YO(t,n){}function ZO(t,n){if(t&1&&$(0,YO,0,0,"ng-template",13),t&2){w();let e=ri(1);E("ngTemplateOutlet",e)}}function KO(t,n){t&1&&(p(0,"div",14,4),ue(2,4),h())}function QO(t,n){t&1&&(p(0,"div",15,5),ue(2,5),h())}function XO(t,n){t&1&&pe(0,"div",16)}function JO(t,n){t&1&&(p(0,"div",18),ue(1,6),h())}function e1(t,n){if(t&1&&(p(0,"mat-hint",22),_(1),h()),t&2){let e=w(2);E("id",e._hintLabelId),v(),nt(e.hintLabel)}}function t1(t,n){if(t&1&&(p(0,"div",19),_e(1,e1,2,2,"mat-hint",22),ue(2,7),pe(3,"div",23),ue(4,8),h()),t&2){let e=w();v(),ye(e.hintLabel?1:-1)}}var lt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["mat-label"]]})}return t})(),n1=new y("MatError");var Ac=(()=>{class t{align="start";id=u(mt).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(wn("id",r.id),he("align",null),ce("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),Pw=new y("MatPrefix"),Co=(()=>{class t{set _isTextSelector(e){this._isText=!0}_isText=!1;static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["","matPrefix",""],["","matIconPrefix",""],["","matTextPrefix",""]],inputs:{_isTextSelector:[0,"matTextPrefix","_isTextSelector"]},features:[ze([{provide:Pw,useExisting:t}])]})}return t})(),Lw=new y("MatSuffix"),fn=(()=>{class t{set _isTextSelector(e){this._isText=!0}_isText=!1;static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["","matSuffix",""],["","matIconSuffix",""],["","matTextSuffix",""]],inputs:{_isTextSelector:[0,"matTextSuffix","_isTextSelector"]},features:[ze([{provide:Lw,useExisting:t}])]})}return t})(),Vw=new y("FloatingLabelParent"),Aw=(()=>{class t{_elementRef=u(j);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=u(xw);_ngZone=u(O);_parent=u(Vw);_resizeSubscription=new fe;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return i1(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&ce("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function i1(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var Rw="mdc-line-ripple--active",sf="mdc-line-ripple--deactivating",Nw=(()=>{class t{_elementRef=u(j);_cleanupTransitionEnd;constructor(){let e=u(O),i=u(je);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(sf),e.add(Rw)}deactivate(){this._elementRef.nativeElement.classList.add(sf)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(sf);e.propertyName==="opacity"&&r&&i.remove(Rw,sf)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),kw=(()=>{class t{_elementRef=u(j);_ngZone=u(O);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&Nt(OO,5),i&2){let o;K(o=Q())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&ce("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:FO,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(He(),En(0,"div",1),ft(1,"div",2,0),ue(3),wt(),En(4,"div",3))},encapsulation:2})}return t})(),Rc=(()=>{class t{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t})}return t})();var Nc=new y("MatFormField"),r1=new y("MAT_FORM_FIELD_DEFAULT_OPTIONS"),Ow="fill",o1="auto",Fw="fixed",s1="translateY(-50%)",vt=(()=>{class t{_elementRef=u(j);_changeDetectorRef=u(me);_platform=u($e);_idGenerator=u(mt);_ngZone=u(O);_defaults=u(r1,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=ja("iconPrefixContainer");_textPrefixContainerSignal=ja("textPrefixContainer");_iconSuffixContainerSignal=ja("iconSuffixContainer");_textSuffixContainerSignal=ja("textSuffixContainer");_prefixSuffixContainers=qt(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=q0(lt);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=As(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||o1}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||Ow;this._appearanceSignal.set(i)}_appearanceSignal=ee(Ow);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||Fw}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||Fw}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new S;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=St();constructor(){let e=this._defaults,i=u(An);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),Bn(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=qt(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(_t([void 0,void 0]),ne(()=>[i.errorState,i.userAriaDescribedBy]),Sl(),Se(([[o,s],[a,c]])=>o!==a||s!==c)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(ke(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),Qt(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){K0({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=qt(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(s=>s&&!o.includes(s)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,c=r?.getBoundingClientRect().width??0,l=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",f=`${s+a}px`,g=`calc(${d} * (${f} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,b=`var(--mat-mdc-form-field-label-transform, ${s1} translateX(${g}))`,I=s+a+c+l;return[b,I]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&($d(o,r._labelChild,lt,5),Dn(o,Rc,5)(o,Pw,5)(o,Lw,5)(o,n1,5)(o,Ac,5)),i&2){Wd();let s;K(s=Q())&&(r._formFieldControl=s.first),K(s=Q())&&(r._prefixChildren=s),K(s=Q())&&(r._suffixChildren=s),K(s=Q())&&(r._errorChildren=s),K(s=Q())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(Gd(r._iconPrefixContainerSignal,Iw,5)(r._textPrefixContainerSignal,Sw,5)(r._iconSuffixContainerSignal,Mw,5)(r._textSuffixContainerSignal,Tw,5),Nt(PO,5)(Iw,5)(Sw,5)(Mw,5)(Tw,5)(Aw,5)(kw,5)(Nw,5)),i&2){Wd(4);let o;K(o=Q())&&(r._textField=o.first),K(o=Q())&&(r._iconPrefixContainer=o.first),K(o=Q())&&(r._textPrefixContainer=o.first),K(o=Q())&&(r._iconSuffixContainer=o.first),K(o=Q())&&(r._textSuffixContainer=o.first),K(o=Q())&&(r._floatingLabel=o.first),K(o=Q())&&(r._notchedOutline=o.first),K(o=Q())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&ce("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[ze([{provide:Nc,useExisting:t},{provide:Vw,useExisting:t}])],ngContentSelectors:VO,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(He(LO),$(0,UO,1,1,"ng-template",null,0,La),p(2,"div",6,1),F("click",function(s){return r._control.onContainerClick(s)}),_e(4,HO,1,0,"div",7),p(5,"div",8),_e(6,GO,2,2,"div",9),_e(7,WO,3,0,"div",10),_e(8,qO,3,0,"div",11),p(9,"div",12),_e(10,ZO,1,1,null,13),ue(11),h(),_e(12,KO,3,0,"div",14),_e(13,QO,3,0,"div",15),h(),_e(14,XO,1,0,"div",16),h(),p(15,"div",17),_e(16,JO,2,0,"div",18)(17,t1,5,1,"div",19),h()),i&2){let o;v(2),ce("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),v(2),ye(!r._hasOutline()&&!r._control.disabled?4:-1),v(2),ye(r._hasOutline()?6:-1),v(),ye(r._hasIconPrefix?7:-1),v(),ye(r._hasTextPrefix?8:-1),v(2),ye(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),v(2),ye(r._hasTextSuffix?12:-1),v(),ye(r._hasIconSuffix?13:-1),v(),ye(r._hasOutline()?-1:14),v(),ce("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();v(),ye((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[Aw,kw,Ua,Nw,Ac],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2})}return t})();var Rs=(()=>{class t{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Ns=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(n,e,i,r,o){this._defaultMatcher=n,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let n=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==n&&(this.errorState=o,this._stateChanges.next())}};var Je=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({imports:[sw,vt,Ie]})}return t})();var a1=["button","checkbox","file","hidden","image","radio","range","reset","submit"],c1=new y("MAT_INPUT_CONFIG"),jt=(()=>{class t{_elementRef=u(j);_platform=u($e);ngControl=u(ci,{optional:!0,self:!0});_autofillMonitor=u(Ew);_ngZone=u(O);_formField=u(Nc,{optional:!0});_renderer=u(je);_uid=u(mt).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=u(c1,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new S;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=As(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(po.required)??!1}set required(e){this._required=As(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&Yg().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=As(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>Yg().has(e));constructor(){let e=u(Zt,{optional:!0}),i=u(_c,{optional:!0}),r=u(Rs),o=u(Dw,{optional:!0,self:!0}),s=this._elementRef.nativeElement,a=s.nodeName.toLowerCase();o?yn(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Ns(r,this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=a==="select",this._isTextarea=a==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&Bn(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){a1.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&F("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(wn("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),he("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),ce("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",B]},exportAs:["matInput"],features:[ze([{provide:Rc,useExisting:t}]),tt]})}return t})(),Bt=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({imports:[Je,Je,ww,Ie]})}return t})();function jw(t){return Error(`Unable to find icon with the name "${t}"`)}function l1(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function Bw(t){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${t}".`)}function Uw(t){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${t}".`)}var Li=class{url;svgText;options;svgElement=null;constructor(n,e,i){this.url=n,this.svgText=e,this.options=i}},zw=(()=>{class t{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new Li(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let s=this._sanitizer.sanitize(at.HTML,r);if(!s)throw Uw(r);let a=vo(s);return this._addSvgIconConfig(e,i,new Li("",a,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new Li(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(at.HTML,i);if(!o)throw Uw(i);let s=vo(o);return this._addSvgIconSetConfig(e,new Li("",s,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(at.RESOURCE_URL,e);if(!i)throw Bw(e);let r=this._cachedIconsByUrl.get(i);return r?L(af(r)):this._loadSvgIconFromConfig(new Li(e,null)).pipe(Ze(o=>this._cachedIconsByUrl.set(i,o)),ne(o=>af(o)))}getNamedSvgIcon(e,i=""){let r=Hw(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(e,s):Js(jw(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?L(af(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(ne(i=>af(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return L(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(Or(a=>{let l=`Loading icon set URL: ${this._sanitizer.sanitize(at.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(l)),L(null)})));return ta(o).pipe(ne(()=>{let s=this._extractIconWithNameFromAnySet(e,i);if(!s)throw jw(e);return s}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,e,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(Ze(i=>e.svgText=i),ne(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?L(null):this._fetchIcon(e).pipe(Ze(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(vo("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(vo("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw l1();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(at.RESOURCE_URL,i);if(!s)throw Bw(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let c=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(ne(l=>vo(l)),Fr(()=>this._inProgressUrlFetches.delete(s)),na());return this._inProgressUrlFetches.set(s,c),c}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(Hw(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return d1(o)?new Li(o.url,null,o.options):new Li(o,null)}}static \u0275fac=function(i){return new(i||t)(M(Sn,8),M(Za),M(U,8),M(Wt))};static \u0275prov=V({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();function af(t){return t.cloneNode(!0)}function Hw(t,n){return t+":"+n}function d1(t){return!!(t.url&&t.options)}var u1=["*"],f1=new y("MAT_ICON_DEFAULT_OPTIONS"),h1=new y("mat-icon-location",{providedIn:"root",factory:()=>{let t=u(U),n=t?t.location:null;return{getPathname:()=>n?n.pathname+n.search:""}}}),$w=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],p1=$w.map(t=>`[${t}]`).join(", "),m1=/^url\(['"]?#(.*?)['"]?\)$/,Ut=(()=>{class t{_elementRef=u(j);_iconRegistry=u(zw);_location=u(h1);_errorHandler=u(Wt);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=fe.EMPTY;constructor(){let e=u(new nr("aria-hidden"),{optional:!0}),i=u(f1,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${e}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(p1),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)$w.forEach(s=>{let a=i[o],c=a.getAttribute(s),l=c?c.match(m1):null;if(l){let d=r.get(a);d||(d=[],r.set(a,d)),d.push({name:s,value:l[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(Gt(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(he("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),an(r.color?"mat-"+r.color:""),ce("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",B],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:u1,decls:1,vars:0,template:function(i,r){i&1&&(He(),ue(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2})}return t})(),Ht=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({imports:[Ie]})}return t})();var Ge={production:!1,apiUrl:"http://localhost:8080/tela-login-angular"};var kn=class t{constructor(n){this.http=n}http;login(n,e){return this.http.post(`${Ge.apiUrl}/auth/login`,{email:n,senha:e}).pipe(Ze(i=>{localStorage.setItem("token",i.token),localStorage.setItem("id",i.id),localStorage.setItem("nome",i.nome),localStorage.setItem("email",i.email),localStorage.setItem("role",i.role)}))}logout(){let n=localStorage.getItem("token");localStorage.clear(),n&&this.http.post(`${Ge.apiUrl}/auth/logout`,{},{headers:{Authorization:`Bearer ${n}`}}).subscribe()}getToken(){return localStorage.getItem("token")}isLoggedIn(){return!!this.getToken()}novoUsuario(n,e,i){return this.http.post(`${Ge.apiUrl}/usuario`,{nome:n,email:e,senha:i})}static \u0275fac=function(e){return new(e||t)(M(Sn))};static \u0275prov=V({token:t,factory:t.\u0275fac,providedIn:"root"})};function g1(t,n){if(t&1&&(p(0,"div",14),_(1),h()),t&2){let e=w();v(),be(" ",e.mensagens," ")}}function v1(t,n){t&1&&(p(0,"mat-icon"),_(1," login "),h())}var cf=class t{constructor(n,e,i){this.authService=n;this.router=e;this.cdr=i}authService;router;cdr;email="";senha="";mensagens="";carregando=!1;onSubmit(){this.carregando=!0,this.mensagens="",this.authService.login(this.email,this.senha).subscribe({next:n=>{console.log("Login realizado com sucesso",n),this.carregando=!1,this.cdr.detectChanges(),this.router.navigate(["/pets"])},error:n=>{console.error("Falha no login",n),this.carregando=!1,n.status===401?this.mensagens=n.error?.erro??"Email ou senha invalidos.":n.status===0?this.mensagens=n.error?.erro??"Nao foi possivel conectar ao servidor.":this.mensagens=n.error?.erro??"Erro inesperado. Tente novamente.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(x(kn),x(Ve),x(me))};static \u0275cmp=R({type:t,selectors:[["app-login"]],decls:30,vars:6,consts:[[1,"login-container"],[1,"login-card"],[1,"login-header"],[1,"login-icon"],[3,"ngSubmit"],["appearance","outline"],["matInput","","type","email","name","email","required","",3,"ngModelChange","ngModel"],["matSuffix",""],["matInput","","type","password","name","senha","required","",3,"ngModelChange","ngModel"],["class","error",4,"ngIf"],["mat-raised-button","","color","primary","type","submit",1,"full-width",3,"disabled"],[4,"ngIf"],[1,"switch-link"],["routerLink","/criar-usuario"],[1,"error"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card",1)(2,"div",2)(3,"mat-icon",3),_(4," pets "),h(),p(5,"h2"),_(6,"Entrar"),h(),p(7,"p"),_(8,"Bem-vindo ao PetManager"),h()(),p(9,"form",4),F("ngSubmit",function(){return i.onSubmit()}),p(10,"mat-form-field",5)(11,"mat-label"),_(12,"Email"),h(),p(13,"input",6),Ne("ngModelChange",function(o){return Pe(i.email,o)||(i.email=o),o}),h(),Oe(),p(14,"mat-icon",7),_(15," email "),h()(),p(16,"mat-form-field",5)(17,"mat-label"),_(18,"Senha"),h(),p(19,"input",8),Ne("ngModelChange",function(o){return Pe(i.senha,o)||(i.senha=o),o}),h(),Oe(),p(20,"mat-icon",7),_(21," lock "),h()(),$(22,g1,2,1,"div",9),p(23,"button",10),$(24,v1,2,0,"mat-icon",11),_(25),h()(),p(26,"div",12),_(27," N\xE3o possui uma conta? "),p(28,"a",13),_(29," Criar conta "),h()()()()),e&2&&(v(13),Re("ngModel",i.email),Fe(),v(6),Re("ngModel",i.senha),Fe(),v(3),E("ngIf",i.mensagens),v(),E("disabled",i.carregando),v(),E("ngIf",!i.carregando),v(),be(" ",i.carregando?"Entrando...":"Entrar"," "))},dependencies:[Ft,di,pt,Ot,li,Tn,Dt,Zt,it,ct,rt,Lt,Pt,gt,Vt,Je,vt,lt,fn,Bt,jt,Ht,Ut],encapsulation:2})};function _1(t,n){if(t&1&&(p(0,"p"),_(1),h()),t&2){let e=w();v(),be(" ",e.mensagens," ")}}var lf=class t{constructor(n,e,i){this.authService=n;this.router=e;this.cdr=i}authService;router;cdr;nome="";email="";senha="";mensagens="";onSubmit(){this.authService.novoUsuario(this.nome,this.email,this.senha).subscribe({next:n=>{console.log("Usuario criado com sucesso",n),this.mensagens="Usuario criado com sucesso.",this.router.navigate(["/login"]),this.cdr.detectChanges()},error:n=>{console.error("Nao foi possivel criar este usuario",n),this.mensagens=n.error?.erro??"Nao foi possivel criar este usuario. Verifique se as informa\xE7\xF5es s\xE3o v\xE1lidas",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(x(kn),x(Ve),x(me))};static \u0275cmp=R({type:t,selectors:[["app-criar-usuario"]],decls:24,vars:4,consts:[[1,"login-container"],[1,"login-card"],[3,"ngSubmit"],["appearance","outline"],["matInput","","name","nome","required","",3,"ngModelChange","ngModel"],["matInput","","type","email","name","email","required","",3,"ngModelChange","ngModel"],["matInput","","type","password","name","senha","required","",3,"ngModelChange","ngModel"],["mat-raised-button","","color","primary","type","submit",1,"full-width"],[4,"ngIf"],[1,"switch-link"],["routerLink","/login"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card",1)(2,"h2"),_(3,"Criar conta"),h(),p(4,"form",2),F("ngSubmit",function(){return i.onSubmit()}),p(5,"mat-form-field",3)(6,"mat-label"),_(7,"Nome"),h(),p(8,"input",4),Ne("ngModelChange",function(o){return Pe(i.nome,o)||(i.nome=o),o}),h(),Oe(),h(),p(9,"mat-form-field",3)(10,"mat-label"),_(11,"Email"),h(),p(12,"input",5),Ne("ngModelChange",function(o){return Pe(i.email,o)||(i.email=o),o}),h(),Oe(),h(),p(13,"mat-form-field",3)(14,"mat-label"),_(15,"Senha (m\xEDnimo de 4 caracteres)"),h(),p(16,"input",6),Ne("ngModelChange",function(o){return Pe(i.senha,o)||(i.senha=o),o}),h(),Oe(),h(),p(17,"button",7),_(18," Criar conta "),h()(),$(19,_1,2,1,"p",8),p(20,"div",9),_(21," J\xE1 possui uma conta? "),p(22,"a",10),_(23," Entrar "),h()()()()),e&2&&(v(8),Re("ngModel",i.nome),Fe(),v(4),Re("ngModel",i.email),Fe(),v(4),Re("ngModel",i.senha),Fe(),v(3),E("ngIf",i.mensagens))},dependencies:[Ft,di,pt,Ot,li,Tn,Dt,Zt,it,ct,rt,Lt,Pt,Je,vt,lt,Bt,jt,gt,Vt],styles:[".full-width[_ngcontent-%COMP%]{width:100%}.switch-link[_ngcontent-%COMP%]{margin-top:20px;text-align:center}.switch-link[_ngcontent-%COMP%]   a[_ngcontent-%COMP%]{font-weight:600}"]})};var y1=["determinateSpinner"];function b1(t,n){if(t&1&&(ei(),p(0,"svg",11),pe(1,"circle",12),h()),t&2){let e=w();he("viewBox",e._viewBox()),v(),ro("stroke-dasharray",e._strokeCircumference(),"px")("stroke-dashoffset",e._strokeCircumference()/2,"px")("stroke-width",e._circleStrokeWidth(),"%"),he("r",e._circleRadius())}}var C1=new y("mat-progress-spinner-default-options",{providedIn:"root",factory:()=>({diameter:Gw})}),Gw=100,E1=10,On=(()=>{class t{_elementRef=u(j);_noopAnimations;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;_defaultColor="primary";_determinateCircle;constructor(){let e=u(C1),i=Zg(),r=this._elementRef.nativeElement;this._noopAnimations=i==="di-disabled"&&!!e&&!e._forceAnimations,this.mode=r.nodeName.toLowerCase()==="mat-spinner"?"indeterminate":"determinate",!this._noopAnimations&&i==="reduced-motion"&&r.classList.add("mat-progress-spinner-reduced-motion"),e&&(e.color&&(this.color=this._defaultColor=e.color),e.diameter&&(this.diameter=e.diameter),e.strokeWidth&&(this.strokeWidth=e.strokeWidth))}mode;get value(){return this.mode==="determinate"?this._value:0}set value(e){this._value=Math.max(0,Math.min(100,e||0))}_value=0;get diameter(){return this._diameter}set diameter(e){this._diameter=e||0}_diameter=Gw;get strokeWidth(){return this._strokeWidth??this.diameter/10}set strokeWidth(e){this._strokeWidth=e||0}_strokeWidth;_circleRadius(){return(this.diameter-E1)/2}_viewBox(){let e=this._circleRadius()*2+this.strokeWidth;return`0 0 ${e} ${e}`}_strokeCircumference(){return 2*Math.PI*this._circleRadius()}_strokeDashOffset(){return this.mode==="determinate"?this._strokeCircumference()*(100-this._value)/100:null}_circleStrokeWidth(){return this.strokeWidth/this.diameter*100}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-progress-spinner"],["mat-spinner"]],viewQuery:function(i,r){if(i&1&&Nt(y1,5),i&2){let o;K(o=Q())&&(r._determinateCircle=o.first)}},hostAttrs:["role","progressbar","tabindex","-1",1,"mat-mdc-progress-spinner","mdc-circular-progress"],hostVars:18,hostBindings:function(i,r){i&2&&(he("aria-valuemin",0)("aria-valuemax",100)("aria-valuenow",r.mode==="determinate"?r.value:null)("mode",r.mode),an("mat-"+r.color),ro("width",r.diameter,"px")("height",r.diameter,"px")("--mat-progress-spinner-size",r.diameter+"px")("--mat-progress-spinner-active-indicator-width",r.diameter+"px"),ce("_mat-animation-noopable",r._noopAnimations)("mdc-circular-progress--indeterminate",r.mode==="indeterminate"))},inputs:{color:"color",mode:"mode",value:[2,"value","value",xn],diameter:[2,"diameter","diameter",xn],strokeWidth:[2,"strokeWidth","strokeWidth",xn]},exportAs:["matProgressSpinner"],decls:14,vars:11,consts:[["circle",""],["determinateSpinner",""],["aria-hidden","true",1,"mdc-circular-progress__determinate-container"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__determinate-circle-graphic"],["cx","50%","cy","50%",1,"mdc-circular-progress__determinate-circle"],["aria-hidden","true",1,"mdc-circular-progress__indeterminate-container"],[1,"mdc-circular-progress__spinner-layer"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-left"],[3,"ngTemplateOutlet"],[1,"mdc-circular-progress__gap-patch"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-right"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__indeterminate-circle-graphic"],["cx","50%","cy","50%"]],template:function(i,r){if(i&1&&($(0,b1,2,8,"ng-template",null,0,La),p(2,"div",2,1),ei(),p(4,"svg",3),pe(5,"circle",4),h()(),ed(),p(6,"div",5)(7,"div",6)(8,"div",7),os(9,8),h(),p(10,"div",9),os(11,8),h(),p(12,"div",10),os(13,8),h()()()),i&2){let o=ri(1);v(4),he("viewBox",r._viewBox()),v(),ro("stroke-dasharray",r._strokeCircumference(),"px")("stroke-dashoffset",r._strokeDashOffset(),"px")("stroke-width",r._circleStrokeWidth(),"%"),he("r",r._circleRadius()),v(4),E("ngTemplateOutlet",o),v(2),E("ngTemplateOutlet",o),v(2),E("ngTemplateOutlet",o)}},dependencies:[Ua],styles:[`.mat-mdc-progress-spinner {
  --mat-progress-spinner-animation-multiplier: 1;
  display: block;
  overflow: hidden;
  line-height: 0;
  position: relative;
  direction: ltr;
  transition: opacity 250ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-progress-spinner circle {
  stroke-width: var(--mat-progress-spinner-active-indicator-width, 4px);
}
.mat-mdc-progress-spinner._mat-animation-noopable, .mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__determinate-circle {
  transition: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-circle-graphic,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__spinner-layer,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container {
  animation: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container circle {
  stroke-dasharray: 0 !important;
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic,
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle {
    stroke: currentColor;
    stroke: CanvasText;
  }
}

.mat-progress-spinner-reduced-motion {
  --mat-progress-spinner-animation-multiplier: 1.25;
}

.mdc-circular-progress__determinate-container,
.mdc-circular-progress__indeterminate-circle-graphic,
.mdc-circular-progress__indeterminate-container,
.mdc-circular-progress__spinner-layer {
  position: absolute;
  width: 100%;
  height: 100%;
}

.mdc-circular-progress__determinate-container {
  transform: rotate(-90deg);
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__determinate-container {
  opacity: 0;
}

.mdc-circular-progress__indeterminate-container {
  font-size: 0;
  letter-spacing: 0;
  white-space: nowrap;
  opacity: 0;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__indeterminate-container {
  opacity: 1;
  animation: mdc-circular-progress-container-rotate calc(1568.2352941176ms * var(--mat-progress-spinner-animation-multiplier)) linear infinite;
}

.mdc-circular-progress__determinate-circle-graphic,
.mdc-circular-progress__indeterminate-circle-graphic {
  fill: transparent;
}

.mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
.mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
  stroke: var(--mat-progress-spinner-active-indicator-color, var(--mat-sys-primary));
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
    stroke: CanvasText;
  }
}

.mdc-circular-progress__determinate-circle {
  transition: stroke-dashoffset 500ms cubic-bezier(0, 0, 0.2, 1);
}

.mdc-circular-progress__gap-patch {
  position: absolute;
  top: 0;
  left: 47.5%;
  box-sizing: border-box;
  width: 5%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress__gap-patch .mdc-circular-progress__indeterminate-circle-graphic {
  left: -900%;
  width: 2000%;
  transform: rotate(180deg);
}
.mdc-circular-progress__circle-clipper .mdc-circular-progress__indeterminate-circle-graphic {
  width: 200%;
}
.mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  left: -100%;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-left .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-left-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-right-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

.mdc-circular-progress__circle-clipper {
  display: inline-flex;
  position: relative;
  width: 50%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress--indeterminate .mdc-circular-progress__spinner-layer {
  animation: mdc-circular-progress-spinner-layer-rotate calc(5332ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

@keyframes mdc-circular-progress-container-rotate {
  to {
    transform: rotate(360deg);
  }
}
@keyframes mdc-circular-progress-spinner-layer-rotate {
  12.5% {
    transform: rotate(135deg);
  }
  25% {
    transform: rotate(270deg);
  }
  37.5% {
    transform: rotate(405deg);
  }
  50% {
    transform: rotate(540deg);
  }
  62.5% {
    transform: rotate(675deg);
  }
  75% {
    transform: rotate(810deg);
  }
  87.5% {
    transform: rotate(945deg);
  }
  100% {
    transform: rotate(1080deg);
  }
}
@keyframes mdc-circular-progress-left-spin {
  from {
    transform: rotate(265deg);
  }
  50% {
    transform: rotate(130deg);
  }
  to {
    transform: rotate(265deg);
  }
}
@keyframes mdc-circular-progress-right-spin {
  from {
    transform: rotate(-265deg);
  }
  50% {
    transform: rotate(-130deg);
  }
  to {
    transform: rotate(-265deg);
  }
}
`],encapsulation:2})}return t})();var Fn=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({imports:[Ie]})}return t})();var w1=["*",[["mat-chip-avatar"],["","matChipAvatar",""]],[["mat-chip-trailing-icon"],["","matChipRemove",""],["","matChipTrailingIcon",""]]],D1=["*","mat-chip-avatar, [matChipAvatar]","mat-chip-trailing-icon,[matChipRemove],[matChipTrailingIcon]"];function x1(t,n){t&1&&(p(0,"span",3),ue(1,1),h())}function I1(t,n){t&1&&(p(0,"span",6),ue(1,2),h())}var S1=["*"];var M1=new y("mat-chips-default-options",{providedIn:"root",factory:()=>({separatorKeyCodes:[13]})}),Ww=new y("MatChipAvatar"),qw=new y("MatChipTrailingIcon"),Yw=new y("MatChipEdit"),nv=new y("MatChipRemove"),Zw=new y("MatChip"),Kw=(()=>{class t{_elementRef=u(j);_parentChip=u(Zw);_isPrimary=!0;_isLeading=!1;get disabled(){return this._disabled||this._parentChip?.disabled||!1}set disabled(e){this._disabled=e}_disabled=!1;tabIndex=-1;_allowFocusWhenDisabled=!1;_getDisabledAttribute(){return this.disabled&&!this._allowFocusWhenDisabled?"":null}constructor(){u(It).load(Oi),this._elementRef.nativeElement.nodeName==="BUTTON"&&this._elementRef.nativeElement.setAttribute("type","button")}focus(){this._elementRef.nativeElement.focus()}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["","matChipContent",""]],hostAttrs:[1,"mat-mdc-chip-action","mdc-evolution-chip__action","mdc-evolution-chip__action--presentational"],hostVars:8,hostBindings:function(i,r){i&2&&(he("disabled",r._getDisabledAttribute())("aria-disabled",r.disabled),ce("mdc-evolution-chip__action--primary",r._isPrimary)("mdc-evolution-chip__action--secondary",!r._isPrimary)("mdc-evolution-chip__action--trailing",!r._isPrimary&&!r._isLeading))},inputs:{disabled:[2,"disabled","disabled",B],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?-1:xn(e)],_allowFocusWhenDisabled:"_allowFocusWhenDisabled"}})}return t})(),Qw=(()=>{class t extends Kw{_getTabindex(){return this.disabled&&!this._allowFocusWhenDisabled?null:this.tabIndex.toString()}_handleClick(e){!this.disabled&&this._isPrimary&&(e.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!this.disabled&&this._isPrimary&&!this._parentChip._isEditing&&(e.preventDefault(),this._parentChip._handlePrimaryActionInteraction())}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Gn(t)))(r||t)}})();static \u0275dir=k({type:t,selectors:[["","matChipAction",""]],hostVars:3,hostBindings:function(i,r){i&1&&F("click",function(s){return r._handleClick(s)})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(he("tabindex",r._getTabindex()),ce("mdc-evolution-chip__action--presentational",!1))},features:[Be]})}return t})();var Xw=(()=>{class t extends Qw{_isPrimary=!1;_handleClick(e){this.disabled||(e.stopPropagation(),e.preventDefault(),this._parentChip.remove())}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!this.disabled&&(e.stopPropagation(),e.preventDefault(),this._parentChip.remove())}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Gn(t)))(r||t)}})();static \u0275dir=k({type:t,selectors:[["","matChipRemove",""]],hostAttrs:["role","button",1,"mat-mdc-chip-remove","mat-mdc-chip-trailing-icon","mat-focus-indicator","mdc-evolution-chip__icon","mdc-evolution-chip__icon--trailing"],hostVars:1,hostBindings:function(i,r){i&2&&he("aria-hidden",null)},features:[ze([{provide:nv,useExisting:t}]),Be]})}return t})(),Oc=(()=>{class t{_changeDetectorRef=u(me);_elementRef=u(j);_tagName=u(W0);_ngZone=u(O);_focusMonitor=u(fr);_globalRippleOptions=u(Tc,{optional:!0});_document=u(U);_onFocus=new S;_onBlur=new S;_isBasicChip=!1;role=null;_hasFocusInternal=!1;_pendingFocus=!1;_actionChanges;_animationsDisabled=St();_allLeadingIcons;_allTrailingIcons;_allEditIcons;_allRemoveIcons;_hasFocus(){return this._hasFocusInternal}id=u(mt).getId("mat-mdc-chip-");ariaLabel=null;ariaDescription=null;_chipListDisabled=!1;_hadFocusOnRemove=!1;_textElement;get value(){return this._value!==void 0?this._value:this._textElement.textContent.trim()}set value(e){this._value=e}_value;color;removable=!0;highlighted=!1;disableRipple=!1;get disabled(){return this._disabled||this._chipListDisabled}set disabled(e){this._disabled=e}_disabled=!1;removed=new Y;destroyed=new Y;basicChipAttrName="mat-basic-chip";leadingIcon;editIcon;trailingIcon;removeIcon;primaryAction;_rippleLoader=u(of);_injector=u(ae);constructor(){let e=u(It);e.load(Oi),e.load(Cc),this._monitorFocus(),this._rippleLoader?.configureRipple(this._elementRef.nativeElement,{className:"mat-mdc-chip-ripple",disabled:this._isRippleDisabled()})}ngOnInit(){this._isBasicChip=this._elementRef.nativeElement.hasAttribute(this.basicChipAttrName)||this._tagName.toLowerCase()===this.basicChipAttrName}ngAfterViewInit(){this._textElement=this._elementRef.nativeElement.querySelector(".mat-mdc-chip-action-label"),this._pendingFocus&&(this._pendingFocus=!1,this.focus())}ngAfterContentInit(){this._actionChanges=Qt(this._allLeadingIcons.changes,this._allTrailingIcons.changes,this._allEditIcons.changes,this._allRemoveIcons.changes).subscribe(()=>this._changeDetectorRef.markForCheck())}ngDoCheck(){this._rippleLoader.setDisabled(this._elementRef.nativeElement,this._isRippleDisabled())}ngOnDestroy(){this.destroyed.emit({chip:this}),this.destroyed.complete(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement),this._actionChanges?.unsubscribe()}remove(){this.removable&&(this._hadFocusOnRemove=this._hasFocus(),this.removed.emit({chip:this}))}_isRippleDisabled(){return this.disabled||this.disableRipple||this._animationsDisabled||this._isBasicChip||!this._hasInteractiveActions()||!!this._globalRippleOptions?.disabled}_hasTrailingIcon(){return!!(this.trailingIcon||this.removeIcon)}_handleKeydown(e){(e.keyCode===8&&!e.repeat||e.keyCode===46)&&(e.preventDefault(),this.remove())}focus(){this.disabled||(this.primaryAction?this.primaryAction.focus():this._pendingFocus=!0)}_getSourceAction(e){return this._getActions().find(i=>{let r=i._elementRef.nativeElement;return r===e||r.contains(e)})}_getActions(){let e=[];return this.editIcon&&e.push(this.editIcon),this.primaryAction&&e.push(this.primaryAction),this.removeIcon&&e.push(this.removeIcon),e}_handlePrimaryActionInteraction(){}_hasInteractiveActions(){return this._getActions().length>0}_edit(e){}_monitorFocus(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{let i=e!==null;i!==this._hasFocusInternal&&(this._hasFocusInternal=i,i?this._onFocus.next({chip:this}):(this._changeDetectorRef.markForCheck(),setTimeout(()=>this._ngZone.run(()=>this._onBlur.next({chip:this})))))})}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-basic-chip"],["","mat-basic-chip",""],["mat-chip"],["","mat-chip",""]],contentQueries:function(i,r,o){if(i&1&&Dn(o,Ww,5)(o,Yw,5)(o,qw,5)(o,nv,5)(o,Ww,5)(o,qw,5)(o,Yw,5)(o,nv,5),i&2){let s;K(s=Q())&&(r.leadingIcon=s.first),K(s=Q())&&(r.editIcon=s.first),K(s=Q())&&(r.trailingIcon=s.first),K(s=Q())&&(r.removeIcon=s.first),K(s=Q())&&(r._allLeadingIcons=s),K(s=Q())&&(r._allTrailingIcons=s),K(s=Q())&&(r._allEditIcons=s),K(s=Q())&&(r._allRemoveIcons=s)}},viewQuery:function(i,r){if(i&1&&Nt(Qw,5),i&2){let o;K(o=Q())&&(r.primaryAction=o.first)}},hostAttrs:[1,"mat-mdc-chip"],hostVars:31,hostBindings:function(i,r){i&1&&F("keydown",function(s){return r._handleKeydown(s)}),i&2&&(wn("id",r.id),he("role",r.role)("aria-label",r.ariaLabel),an("mat-"+(r.color||"primary")),ce("mdc-evolution-chip",!r._isBasicChip)("mdc-evolution-chip--disabled",r.disabled)("mdc-evolution-chip--with-trailing-action",r._hasTrailingIcon())("mdc-evolution-chip--with-primary-graphic",r.leadingIcon)("mdc-evolution-chip--with-primary-icon",r.leadingIcon)("mdc-evolution-chip--with-avatar",r.leadingIcon)("mat-mdc-chip-with-avatar",r.leadingIcon)("mat-mdc-chip-highlighted",r.highlighted)("mat-mdc-chip-disabled",r.disabled)("mat-mdc-basic-chip",r._isBasicChip)("mat-mdc-standard-chip",!r._isBasicChip)("mat-mdc-chip-with-trailing-icon",r._hasTrailingIcon())("_mat-animation-noopable",r._animationsDisabled))},inputs:{role:"role",id:"id",ariaLabel:[0,"aria-label","ariaLabel"],ariaDescription:[0,"aria-description","ariaDescription"],value:"value",color:"color",removable:[2,"removable","removable",B],highlighted:[2,"highlighted","highlighted",B],disableRipple:[2,"disableRipple","disableRipple",B],disabled:[2,"disabled","disabled",B]},outputs:{removed:"removed",destroyed:"destroyed"},exportAs:["matChip"],features:[ze([{provide:Zw,useExisting:t}])],ngContentSelectors:D1,decls:8,vars:2,consts:[[1,"mat-mdc-chip-focus-overlay"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--primary"],["matChipContent",""],[1,"mdc-evolution-chip__graphic","mat-mdc-chip-graphic"],[1,"mdc-evolution-chip__text-label","mat-mdc-chip-action-label"],[1,"mat-mdc-chip-primary-focus-indicator","mat-focus-indicator"],[1,"mdc-evolution-chip__cell","mdc-evolution-chip__cell--trailing"]],template:function(i,r){i&1&&(He(w1),pe(0,"span",0),p(1,"span",1)(2,"span",2),_e(3,x1,2,0,"span",3),p(4,"span",4),ue(5),pe(6,"span",5),h()()(),_e(7,I1,2,0,"span",6)),i&2&&(v(3),ye(r.leadingIcon?3:-1),v(4),ye(r._hasTrailingIcon()?7:-1))},dependencies:[Kw],styles:[`.mdc-evolution-chip,
.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  display: inline-flex;
  align-items: center;
}

.mdc-evolution-chip {
  position: relative;
  max-width: 100%;
}

.mdc-evolution-chip__cell,
.mdc-evolution-chip__action {
  height: 100%;
}

.mdc-evolution-chip__cell--primary {
  flex-basis: 100%;
  overflow-x: hidden;
}

.mdc-evolution-chip__cell--trailing {
  flex: 1 0 auto;
}

.mdc-evolution-chip__action {
  align-items: center;
  background: none;
  border: none;
  box-sizing: content-box;
  cursor: pointer;
  display: inline-flex;
  justify-content: center;
  outline: none;
  padding: 0;
  text-decoration: none;
  color: inherit;
}

.mdc-evolution-chip__action--presentational {
  cursor: auto;
}

.mdc-evolution-chip--disabled,
.mdc-evolution-chip__action:disabled {
  pointer-events: none;
}
@media (forced-colors: active) {
  .mdc-evolution-chip--disabled,
  .mdc-evolution-chip__action:disabled {
    forced-color-adjust: none;
  }
}

.mdc-evolution-chip__action--primary {
  font: inherit;
  letter-spacing: inherit;
  white-space: inherit;
  overflow-x: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-outline-width, 1px);
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  pointer-events: none;
  top: 0;
  width: 100%;
  z-index: 1;
  border-style: solid;
}
.mat-mdc-standard-chip .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-outline-color, var(--mat-sys-outline));
}
.mdc-evolution-chip__action--primary:not(.mdc-evolution-chip__action--presentational):not(.mdc-ripple-upgraded):focus::before {
  border-color: var(--mat-chip-focus-outline-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--primary::before {
  border-color: var(--mat-chip-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__action--primary::before {
  border-width: var(--mat-chip-flat-selected-outline-width, 0);
}
.mat-mdc-basic-chip .mdc-evolution-chip__action--primary {
  font: inherit;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 12px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__action--primary {
  padding-left: 12px;
  padding-right: 0;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--primary {
  padding-left: 0;
  padding-right: 0;
}

.mdc-evolution-chip__action--secondary {
  position: relative;
  overflow: visible;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__action--secondary {
  color: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__action--secondary, [dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__action--secondary {
  padding-left: 8px;
  padding-right: 8px;
}

.mdc-evolution-chip__text-label {
  -webkit-user-select: none;
  user-select: none;
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}
.mat-mdc-standard-chip .mdc-evolution-chip__text-label {
  font-family: var(--mat-chip-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-chip-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-chip-label-text-size, var(--mat-sys-label-large-size));
  font-weight: var(--mat-chip-label-text-weight, var(--mat-sys-label-large-weight));
  letter-spacing: var(--mat-chip-label-text-tracking, var(--mat-sys-label-large-tracking));
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-label-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__text-label {
  color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label, .mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__text-label {
  color: var(--mat-chip-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mdc-evolution-chip__graphic {
  align-items: center;
  display: inline-flex;
  justify-content: center;
  overflow: hidden;
  pointer-events: none;
  position: relative;
  flex: 1 0 auto;
}
.mat-mdc-standard-chip .mdc-evolution-chip__graphic {
  width: var(--mat-chip-with-avatar-avatar-size, 24px);
  height: var(--mat-chip-with-avatar-avatar-size, 24px);
  font-size: var(--mat-chip-with-avatar-avatar-size, 24px);
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__graphic {
  transition: width 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selectable:not(.mdc-evolution-chip--selected):not(.mdc-evolution-chip--with-primary-icon) .mdc-evolution-chip__graphic {
  width: 0;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mat-mdc-standard-chip.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 6px;
  padding-right: 6px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 4px;
  padding-right: 8px;
}
[dir=rtl] .mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-trailing-action .mdc-evolution-chip__graphic {
  padding-left: 8px;
  padding-right: 4px;
}
.mdc-evolution-chip--with-avatar.mdc-evolution-chip--with-primary-graphic.mdc-evolution-chip--with-leading-action .mdc-evolution-chip__graphic {
  padding-left: 0;
}

.mdc-evolution-chip__checkmark {
  position: absolute;
  opacity: 0;
  top: 50%;
  left: 50%;
  height: 20px;
  width: 20px;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__checkmark {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark {
  transition: transform 150ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  transform: translate(-75%, -50%);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  transform: translate(-50%, -50%);
  opacity: 1;
}

.mdc-evolution-chip__checkmark-svg {
  display: block;
}

.mdc-evolution-chip__checkmark-path {
  stroke-width: 2px;
  stroke-dasharray: 29.7833385;
  stroke-dashoffset: 29.7833385;
  stroke: currentColor;
}
.mdc-evolution-chip--selecting .mdc-evolution-chip__checkmark-path {
  transition: stroke-dashoffset 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark-path {
  stroke-dashoffset: 0;
}
@media (forced-colors: active) {
  .mdc-evolution-chip__checkmark-path {
    stroke: CanvasText !important;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--trailing {
  height: 18px;
  width: 18px;
  font-size: 18px;
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove {
  opacity: calc(var(--mat-chip-trailing-action-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}
.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing.mat-mdc-chip-remove:focus {
  opacity: calc(var(--mat-chip-trailing-action-focus-opacity, 1) * var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38));
}

.mat-mdc-standard-chip {
  border-radius: var(--mat-chip-container-shape-radius, 8px);
  height: var(--mat-chip-container-height, 32px);
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-container-color, transparent);
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-elevated-disabled-container-color);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected:not(.mdc-evolution-chip--disabled) {
  background-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled {
  background-color: var(--mat-chip-flat-disabled-selected-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-standard-chip {
    outline: solid 1px;
  }
}

.mat-mdc-standard-chip .mdc-evolution-chip__icon--primary {
  border-radius: var(--mat-chip-with-avatar-avatar-shape-radius, 24px);
  width: var(--mat-chip-with-icon-icon-size, 18px);
  height: var(--mat-chip-with-icon-icon-size, 18px);
  font-size: var(--mat-chip-with-icon-icon-size, 18px);
}
.mdc-evolution-chip--selected .mdc-evolution-chip__icon--primary {
  opacity: 0;
}
.mat-mdc-standard-chip:not(.mdc-evolution-chip--disabled) .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-standard-chip.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--primary {
  color: var(--mat-chip-with-icon-disabled-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-highlighted {
  --mat-chip-with-icon-icon-color: var(--mat-chip-with-icon-selected-icon-color, var(--mat-sys-on-secondary-container));
  --mat-chip-elevated-container-color: var(--mat-chip-elevated-selected-container-color, var(--mat-sys-secondary-container));
  --mat-chip-label-text-color: var(--mat-chip-selected-label-text-color, var(--mat-sys-on-secondary-container));
  --mat-chip-outline-width: var(--mat-chip-flat-selected-outline-width, 0);
}

.mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-selected .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-chip:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-hover-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip-focus-overlay .mat-mdc-chip-selected:hover, .mat-mdc-chip-highlighted:hover .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-hover-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-focus-state-layer-color, var(--mat-sys-on-surface-variant));
  opacity: var(--mat-chip-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-chip-selected.cdk-focused .mat-mdc-chip-focus-overlay, .mat-mdc-chip-highlighted.cdk-focused .mat-mdc-chip-focus-overlay {
  background: var(--mat-chip-selected-focus-state-layer-color, var(--mat-sys-on-secondary-container));
  opacity: var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}

.mdc-evolution-chip--disabled:not(.mdc-evolution-chip--selected) .mat-mdc-chip-avatar {
  opacity: var(--mat-chip-with-avatar-disabled-avatar-opacity, 0.38);
}

.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  opacity: var(--mat-chip-with-trailing-icon-disabled-trailing-icon-opacity, 0.38);
}

.mdc-evolution-chip--disabled.mdc-evolution-chip--selected .mdc-evolution-chip__checkmark {
  opacity: var(--mat-chip-with-icon-disabled-icon-opacity, 0.38);
}

.mat-mdc-standard-chip.mdc-evolution-chip--disabled {
  opacity: var(--mat-chip-disabled-container-opacity, 1);
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-trailing-icon-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-standard-chip.mdc-evolution-chip--selected.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing, .mat-mdc-standard-chip.mat-mdc-chip-highlighted.mdc-evolution-chip--disabled .mdc-evolution-chip__icon--trailing {
  color: var(--mat-chip-selected-disabled-trailing-icon-color, var(--mat-sys-on-surface));
}

.mat-mdc-chip-edit, .mat-mdc-chip-remove {
  opacity: var(--mat-chip-trailing-action-opacity, 1);
}
.mat-mdc-chip-edit:focus, .mat-mdc-chip-remove:focus {
  opacity: var(--mat-chip-trailing-action-focus-opacity, 1);
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-trailing-action-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-chip-edit:hover::after, .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}
.mat-mdc-chip-edit:focus::after, .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}

.mat-mdc-chip-selected .mat-mdc-chip-remove::after,
.mat-mdc-chip-highlighted .mat-mdc-chip-remove::after {
  background-color: var(--mat-chip-selected-trailing-action-state-layer-color, var(--mat-sys-on-secondary-container));
}

.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:focus::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:focus::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)));
}
.mat-mdc-chip.cdk-focused .mat-mdc-chip-edit:hover::after, .mat-mdc-chip.cdk-focused .mat-mdc-chip-remove:hover::after {
  opacity: calc(var(--mat-chip-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity)) + var(--mat-chip-trailing-action-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity)));
}

.mat-mdc-standard-chip {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-standard-chip .mat-mdc-chip-graphic,
.mat-mdc-standard-chip .mat-mdc-chip-trailing-icon {
  box-sizing: content-box;
}
.mat-mdc-standard-chip._mat-animation-noopable,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__graphic,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark,
.mat-mdc-standard-chip._mat-animation-noopable .mdc-evolution-chip__checkmark-path {
  transition-duration: 1ms;
  animation-duration: 1ms;
}

.mat-mdc-chip-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  opacity: 0;
  border-radius: inherit;
  transition: opacity 150ms linear;
}
._mat-animation-noopable .mat-mdc-chip-focus-overlay {
  transition: none;
}
.mat-mdc-basic-chip .mat-mdc-chip-focus-overlay {
  display: none;
}

.mat-mdc-chip .mat-ripple.mat-mdc-chip-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}

.mat-mdc-chip-avatar {
  text-align: center;
  line-height: 1;
  color: var(--mat-chip-with-icon-icon-color, currentColor);
}

.mat-mdc-chip {
  position: relative;
  z-index: 0;
}

.mat-mdc-chip-action-label {
  text-align: left;
  z-index: 1;
}
[dir=rtl] .mat-mdc-chip-action-label {
  text-align: right;
}
.mat-mdc-chip.mdc-evolution-chip--with-trailing-action .mat-mdc-chip-action-label {
  position: relative;
}
.mat-mdc-chip-action-label .mat-mdc-chip-primary-focus-indicator {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  pointer-events: none;
}
.mat-mdc-chip-action-label .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-chip-edit::before, .mat-mdc-chip-remove::before {
  margin: calc(var(--mat-focus-indicator-border-width, 3px) * -1);
  left: 8px;
  right: 8px;
}
.mat-mdc-chip-edit::after, .mat-mdc-chip-remove::after {
  content: "";
  display: block;
  opacity: 0;
  position: absolute;
  top: -3px;
  bottom: -3px;
  left: 5px;
  right: 5px;
  border-radius: 50%;
  box-sizing: border-box;
  padding: 12px;
  margin: -12px;
  background-clip: content-box;
}
.mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  width: 18px;
  height: 18px;
  font-size: 18px;
  box-sizing: content-box;
}

.mat-chip-edit-input {
  cursor: text;
  display: inline-block;
  color: inherit;
  outline: 0;
}

@media (forced-colors: active) {
  .mat-mdc-chip-selected:not(.mat-mdc-chip-multiple) {
    outline-width: 3px;
  }
}

.mat-mdc-chip-action:focus-visible .mat-focus-indicator::before {
  content: "";
}

.mdc-evolution-chip__icon, .mat-mdc-chip-edit .mat-icon, .mat-mdc-chip-remove .mat-icon {
  min-height: fit-content;
}

img.mdc-evolution-chip__icon {
  min-height: 0;
}
`],encapsulation:2})}return t})();var df=(()=>{class t{_elementRef=u(j);_changeDetectorRef=u(me);_dir=u(An,{optional:!0});_lastDestroyedFocusedChipIndex=null;_keyManager;_destroyed=new S;_defaultRole="presentation";get chipFocusChanges(){return this._getChipStream(e=>e._onFocus)}get chipDestroyedChanges(){return this._getChipStream(e=>e.destroyed)}get chipRemovedChanges(){return this._getChipStream(e=>e.removed)}get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._syncChipsState()}_disabled=!1;get empty(){return!this._chips||this._chips.length===0}get role(){return this._explicitRole?this._explicitRole:this.empty?null:this._defaultRole}tabIndex=0;set role(e){this._explicitRole=e}_explicitRole=null;get focused(){return this._hasFocusedChip()}_chips;_chipActions=new zn;ngAfterViewInit(){this._setUpFocusManagement(),this._trackChipSetChanges(),this._trackDestroyedFocusedChip()}ngOnDestroy(){this._keyManager?.destroy(),this._chipActions.destroy(),this._destroyed.next(),this._destroyed.complete()}_hasFocusedChip(){return this._chips&&this._chips.some(e=>e._hasFocus())}_syncChipsState(){this._chips?.forEach(e=>{e._chipListDisabled=this._disabled,e._changeDetectorRef.markForCheck()})}focus(){}_handleKeydown(e){this._originatesFromChip(e)&&this._keyManager.onKeydown(e)}_isValidIndex(e){return e>=0&&e<this._chips.length}_allowFocusEscape(){let e=this._elementRef.nativeElement.tabIndex;e!==-1&&(this._elementRef.nativeElement.tabIndex=-1,setTimeout(()=>this._elementRef.nativeElement.tabIndex=e))}_getChipStream(e){return this._chips.changes.pipe(_t(null),We(()=>Qt(...this._chips.map(e))))}_originatesFromChip(e){let i=e.target;for(;i&&i!==this._elementRef.nativeElement;){if(i.classList.contains("mat-mdc-chip"))return!0;i=i.parentElement}return!1}_setUpFocusManagement(){this._chips.changes.pipe(_t(this._chips)).subscribe(e=>{let i=[];e.forEach(r=>r._getActions().forEach(o=>i.push(o))),this._chipActions.reset(i),this._chipActions.notifyOnChanges()}),this._keyManager=new yo(this._chipActions).withVerticalOrientation().withHorizontalOrientation(this._dir?this._dir.value:"ltr").withHomeAndEnd().skipPredicate(e=>this._skipPredicate(e)),this.chipFocusChanges.pipe(ke(this._destroyed)).subscribe(({chip:e})=>{let i=e._getSourceAction(document.activeElement);i&&this._keyManager.updateActiveItem(i)}),this._dir?.change.pipe(ke(this._destroyed)).subscribe(e=>this._keyManager.withHorizontalOrientation(e))}_skipPredicate(e){return e.disabled}_trackChipSetChanges(){this._chips.changes.pipe(_t(null),ke(this._destroyed)).subscribe(()=>{this.disabled&&Promise.resolve().then(()=>this._syncChipsState()),this._redirectDestroyedChipFocus()})}_trackDestroyedFocusedChip(){this.chipDestroyedChanges.pipe(ke(this._destroyed)).subscribe(e=>{let r=this._chips.toArray().indexOf(e.chip),o=e.chip._hasFocus(),s=e.chip._hadFocusOnRemove&&this._keyManager.activeItem&&e.chip._getActions().includes(this._keyManager.activeItem),a=o||s;this._isValidIndex(r)&&a&&(this._lastDestroyedFocusedChipIndex=r)})}_redirectDestroyedChipFocus(){if(this._lastDestroyedFocusedChipIndex!=null){if(this._chips.length){let e=Math.min(this._lastDestroyedFocusedChipIndex,this._chips.length-1),i=this._chips.toArray()[e];i.disabled?this._chips.length===1?this.focus():this._keyManager.setPreviousItemActive():i.focus()}else this.focus();this._lastDestroyedFocusedChipIndex=null}}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-chip-set"]],contentQueries:function(i,r,o){if(i&1&&Dn(o,Oc,5),i&2){let s;K(s=Q())&&(r._chips=s)}},hostAttrs:[1,"mat-mdc-chip-set","mdc-evolution-chip-set"],hostVars:1,hostBindings:function(i,r){i&1&&F("keydown",function(s){return r._handleKeydown(s)}),i&2&&he("role",r.role)},inputs:{disabled:[2,"disabled","disabled",B],role:"role",tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:xn(e)]},ngContentSelectors:S1,decls:2,vars:0,consts:[["role","presentation",1,"mdc-evolution-chip-set__chips"]],template:function(i,r){i&1&&(He(),ft(0,"div",0),ue(1),wt())},styles:[`.mat-mdc-chip-set {
  display: flex;
}
.mat-mdc-chip-set:focus {
  outline: none;
}
.mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  min-width: 100%;
  margin-left: -8px;
  margin-right: 0;
}
.mat-mdc-chip-set .mdc-evolution-chip {
  margin: 4px 0 4px 8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip-set__chips {
  margin-left: 0;
  margin-right: -8px;
}
[dir=rtl] .mat-mdc-chip-set .mdc-evolution-chip {
  margin-left: 0;
  margin-right: 8px;
}

.mdc-evolution-chip-set__chips {
  display: flex;
  flex-flow: wrap;
  min-width: 0;
}

.mat-mdc-chip-set-stacked {
  flex-direction: column;
  align-items: flex-start;
}
.mat-mdc-chip-set-stacked .mat-mdc-chip {
  width: 100%;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__graphic {
  flex-grow: 0;
}
.mat-mdc-chip-set-stacked .mdc-evolution-chip__action--primary {
  flex-basis: 100%;
  justify-content: start;
}

input.mat-mdc-chip-input {
  flex: 1 0 150px;
  margin-left: 8px;
}
[dir=rtl] input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 8px;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-moz-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input::-webkit-input-placeholder {
  opacity: 1;
}
.mat-mdc-form-field:not(.mat-form-field-hide-placeholder) input.mat-mdc-chip-input:-ms-input-placeholder {
  opacity: 1;
}
.mat-mdc-chip-set + input.mat-mdc-chip-input {
  margin-left: 0;
  margin-right: 0;
}
`],encapsulation:2})}return t})();var Os=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({providers:[Rs,{provide:M1,useValue:{separatorKeyCodes:[13]}}],imports:[hr,Ie]})}return t})();var _r=class t{constructor(n){this.http=n}http;todos(){return this.http.get(`${Ge.apiUrl}/usuario/all`)}porId(n){return this.http.get(`${Ge.apiUrl}/usuario/${n}`)}porEmail(n){return this.http.get(`${Ge.apiUrl}/usuario`,{params:{email:n}})}atualizar(n){return this.http.put(`${Ge.apiUrl}/usuario`,n)}remover(n){return this.http.delete(`${Ge.apiUrl}/usuario/${n}`)}static \u0275fac=function(e){return new(e||t)(M(Sn))};static \u0275prov=V({token:t,factory:t.\u0275fac,providedIn:"root"})};var T1=t=>["/usuarios",t];function A1(t,n){if(t&1){let e=Ue();p(0,"button",10),F("click",function(){le(e);let r=w();return r.termoBusca="",de(r.filtrarUsuarios())}),p(1,"mat-icon"),_(2,"close"),h()()}}function R1(t,n){if(t&1&&(p(0,"div",11),_(1),h()),t&2){let e=w();v(),be(" ",e.mensagens," ")}}function N1(t,n){t&1&&(p(0,"div",12),pe(1,"mat-spinner",13),h())}function k1(t,n){if(t&1){let e=Ue();p(0,"tr")(1,"td"),_(2),h(),p(3,"td"),_(4),h(),p(5,"td")(6,"mat-chip-set")(7,"mat-chip",17),_(8),h()()(),p(9,"td",18)(10,"button",19)(11,"mat-icon"),_(12,"edit"),h()(),p(13,"button",20),F("click",function(){let r=le(e).$implicit,o=w(2);return de(o.remover(r.id))}),p(14,"mat-icon"),_(15,"delete"),h()()()()}if(t&2){let e=n.$implicit;v(2),nt(e.nome),v(2),nt(e.email),v(3),E("color",e.role==="ADMIN"?"primary":"accent"),v(),be(" ",e.role," "),v(2),E("routerLink",oo(5,T1,e.id))}}function O1(t,n){if(t&1&&(p(0,"table",14)(1,"thead")(2,"tr")(3,"th"),_(4,"Nome"),h(),p(5,"th"),_(6,"Email"),h(),p(7,"th"),_(8,"Perfil"),h(),p(9,"th",15),_(10,"A\xE7\xF5es"),h()()(),p(11,"tbody"),$(12,k1,16,7,"tr",16),h()()),t&2){let e=w();v(12),E("ngForOf",e.usuariosFiltrados)}}function F1(t,n){t&1&&(p(0,"p"),_(1,"N\xE3o h\xE1 usu\xE1rios cadastrados."),h())}function P1(t,n){t&1&&(p(0,"p"),_(1,"Tente outro termo de busca."),h())}function L1(t,n){if(t&1&&(p(0,"div",21)(1,"mat-icon",22),_(2," group "),h(),p(3,"h3"),_(4),h(),$(5,F1,2,0,"p",23)(6,P1,2,0,"p",23),h()),t&2){let e=w();v(4),nt((e.termoBusca,"Nenhum usu\xE1rio encontrado")),v(),E("ngIf",!e.termoBusca),v(),E("ngIf",e.termoBusca)}}var uf=class t{constructor(n,e,i,r){this.usuarioService=n;this.authService=e;this.cdr=i;this.router=r}usuarioService;authService;cdr;router;usuarios=[];carregando=!0;mensagens="";usuariosFiltrados=[];termoBusca="";ngOnInit(){this.carregar()}carregar(){this.carregando=!0,this.usuarioService.todos().subscribe({next:n=>{this.usuarios=n,this.filtrarUsuarios(),this.carregando=!1,this.cdr.detectChanges()},error:n=>{this.mensagens="Erro ao carregar usu\xE1rios.",this.carregando=!1,this.cdr.detectChanges()}})}filtrarUsuarios(){let n=this.termoBusca.trim().toLowerCase();if(!n){this.usuariosFiltrados=this.usuarios;return}this.usuariosFiltrados=this.usuarios.filter(e=>e.nome?.toLowerCase().includes(n)||e.email?.toLowerCase().includes(n)||e.role?.toLowerCase().includes(n))}remover(n){if(n==null||!confirm("Confirma\xE7ao para remover este usuario"))return;let e=Number(localStorage.getItem("id"))===n;this.usuarioService.remover(n).subscribe({next:()=>{if(e){this.authService.logout(),this.router.navigate(["/login"]),this.cdr.detectChanges();return}this.usuarios=this.usuarios.filter(i=>i.id!==n),this.filtrarUsuarios(),this.cdr.detectChanges()},error:i=>{console.error("Erro ao remover usuario",i),this.mensagens="Nao foi possivel remover este usuario.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(x(_r),x(kn),x(me),x(Ve))};static \u0275cmp=R({type:t,selectors:[["app-gestao-usuarios"]],decls:16,vars:6,consts:[[1,"page"],[1,"header"],["appearance","outline",1,"search-field"],["matInput","","placeholder","Nome, email ou perfil...",3,"ngModelChange","ngModel"],["matPrefix",""],["mat-icon-button","","matSuffix","",3,"click",4,"ngIf"],["class","error",4,"ngIf"],["class","loading",4,"ngIf"],["class","mat-elevation-z2",4,"ngIf"],["class","empty-state",4,"ngIf"],["mat-icon-button","","matSuffix","",3,"click"],[1,"error"],[1,"loading"],["diameter","45"],[1,"mat-elevation-z2"],[2,"width","170px"],[4,"ngFor","ngForOf"],["selected","",3,"color"],[1,"actions"],["mat-icon-button","","color","primary",3,"routerLink"],["mat-icon-button","","color","warn",3,"click"],[1,"empty-state"],[1,"empty-icon"],[4,"ngIf"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card")(2,"div",1)(3,"h2"),_(4,"\u{1F465} Gest\xE3o de Usu\xE1rios"),h()(),p(5,"mat-form-field",2)(6,"mat-label"),_(7,"Pesquisar"),h(),p(8,"input",3),Ne("ngModelChange",function(o){return Pe(i.termoBusca,o)||(i.termoBusca=o),o}),F("ngModelChange",function(){return i.filtrarUsuarios()}),h(),Oe(),p(9,"mat-icon",4),_(10,"search"),h(),$(11,A1,3,0,"button",5),h(),$(12,R1,2,1,"div",6)(13,N1,2,0,"div",7)(14,O1,13,1,"table",8)(15,L1,7,3,"div",9),h()()),e&2&&(v(8),Re("ngModel",i.termoBusca),Fe(),v(3),E("ngIf",i.termoBusca),v(),E("ngIf",i.mensagens),v(),E("ngIf",i.carregando),v(),E("ngIf",!i.carregando&&i.usuariosFiltrados.length>0),v(),E("ngIf",!i.carregando&&i.usuariosFiltrados.length===0))},dependencies:[it,tn,ct,Ft,pt,Ot,Dt,rt,Lt,Pt,gt,Fi,Ht,Ut,Fn,On,Je,vt,lt,Co,fn,Bt,jt,Os,Oc,df],styles:[".mat-mdc-chip[_ngcontent-%COMP%]{font-weight:600}.actions[_ngcontent-%COMP%]{display:flex;gap:8px}.empty-state[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;text-align:center;padding:60px 20px;opacity:.8}.empty-icon[_ngcontent-%COMP%]{font-size:70px!important;width:70px!important;height:70px!important;color:var(--mat-sys-primary);margin-bottom:16px}"]})};var Fc=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new S;bulk={select:n=>this._select(n),deselect:n=>this._deselect(n),setSelection:n=>this._setSelection(n)};constructor(n=!1,e,i=!0,r){this._multiple=n,this._emitChanges=i,this.compareWith=r,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){return this._select(n)}deselect(...n){return this._deselect(n)}setSelection(...n){return this._setSelection(n)}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_select(n){this._verifyValueAssignment(n),n.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_deselect(n){this._verifyValueAssignment(n),n.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_setSelection(n){this._verifyValueAssignment(n);let e=this.selected,i=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(n,i))return i;return n}else return n}};var V1=20,ff=(()=>{class t{_ngZone=u(O);_platform=u($e);_renderer=u(et).createRenderer(null,null);_cleanupGlobalListener;_scrolled=new S;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=V1){return this._platform.isBrowser?new re(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(Dl(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):L()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(Se(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._targetContainsElement(o,e)&&i.push(o)}),i}_targetContainsElement(e,i){let r=un(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var j1=20,yr=(()=>{class t{_platform=u($e);_listeners;_viewportSize=null;_change=new S;_document=u(U);constructor(){let e=u(O),i=u(et).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),s=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(e=j1){return e>0?this._change.pipe(Dl(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();var Fs=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({})}return t})(),rv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({imports:[Ie,Fs,Ie,Fs]})}return t})();var Pc=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},ov=class extends Pc{component;viewContainerRef;injector;projectableNodes;bindings;directives;constructor(n,e,i,r,o,s){super(),this.component=n,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null,this.directives=s||null}},Eo=class extends Pc{templateRef;viewContainerRef;context;injector;constructor(n,e,i,r){super(),this.templateRef=n,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},sv=class extends Pc{element;constructor(n){super(),this.element=n instanceof j?n.nativeElement:n}},av=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof ov)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof Eo)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof sv)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},Lc=class extends av{outletElement;_appRef;_defaultInjector;constructor(n,e,i){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=i}attachComponentPortal(n){let e;if(n.viewContainerRef){let i=n.injector||n.viewContainerRef.injector,r=i.get(xi,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=n.injector||this._defaultInjector||ae.NULL,o=r.get(xe,i.injector);e=Qd(n.component,{elementInjector:r,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,i=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=n,i}attachDomPortal=n=>{let e=n.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var eD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({})}return t})();var tD=uw();function cD(t){return new hf(t.get(yr),t.get(U))}var hf=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=Xe(-this._previousScrollPosition.left),n.style.top=Xe(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,i=n.style,r=e.style,o=i.scrollBehavior||"",s=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),tD&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),tD&&(i.scrollBehavior=o,r.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function lD(t,n){return new pf(t.get(ff),t.get(O),t.get(yr),n)}var pf=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,i,r){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(Se(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var Vc=class{enable(){}disable(){}attach(){}};function cv(t,n){return n.some(e=>{let i=t.bottom<e.top,r=t.top>e.bottom,o=t.right<e.left,s=t.left>e.right;return i||r||o||s})}function nD(t,n){return n.some(e=>{let i=t.top<e.top,r=t.bottom>e.bottom,o=t.left<e.left,s=t.right>e.right;return i||r||o||s})}function xo(t,n){return new mf(t.get(ff),t.get(yr),t.get(O),n)}var mf=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,i,r){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();cv(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},dD=(()=>{class t{_injector=u(ae);noop=()=>new Vc;close=e=>lD(this._injector,e);block=()=>cD(this._injector);reposition=e=>xo(this._injector,e);static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),Do=class{positionStrategy;scrollStrategy=new Vc;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let i of e)n[i]!==void 0&&(this[i]=n[i])}}};var gf=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var uD=(()=>{class t{_attachedOverlays=[];_document=u(U);_isAttached=!1;ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),fD=(()=>{class t extends uD{_ngZone=u(O);_renderer=u(et).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),hD=(()=>{class t extends uD{_platform=u($e);_ngZone=u(O);_renderer=u(et).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=xt(e)};_clickListener=e=>{let i=xt(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],c=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,e,c))){if(iD(a.overlayElement,i)||iD(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>c.next(e)):c.next(e)}}};static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})();function iD(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=n;for(;i;){if(i===t)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var pD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2})}return t})(),mD=(()=>{class t{_platform=u($e);_containerElement;_document=u(U);_styleLoader=u(It);ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||qg()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),qg()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(pD)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),lv=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,i,r){this._renderer=e,this._ngZone=i,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function dv(t){return t&&t.nodeType===1}var vf=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new S;_attachments=new S;_detachments=new S;_positionStrategy;_scrollStrategy;_locationChanges=fe.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new S;_outsidePointerEvents=new S;_afterNextRenderRef;constructor(n,e,i,r,o,s,a,c,l,d=!1,f,m){this._portalOutlet=n,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=c,this._outsideClickDispatcher=l,this._animationsDisabled=d,this._injector=f,this._renderer=m,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=Cn(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=C(C({},this._config),n),this._updateElementSize()}setDirection(n){this._config=H(C({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=Xe(this._config.width),n.height=Xe(this._config.height),n.minWidth=Xe(this._config.minWidth),n.minHeight=Xe(this._config.minHeight),n.maxWidth=Xe(this._config.maxWidth),n.maxHeight=Xe(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;dv(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch(n){}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new lv(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,i){let r=$g(e||[]).filter(o=>!!o);r.length&&(i?n.classList.add(...r):n.classList.remove(...r))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=Cn(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},rD="cdk-overlay-connected-position-bounding-box",B1=/([A-Za-z%]+)$/;function jc(t,n){return new _f(n,t.get(yr),t.get(U),t.get($e),t.get(mD))}var _f=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new S;_resizeSubscription=fe.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(rD),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let c=this._getOriginPoint(n,r,a),l=this._getOverlayPoint(c,e,a),d=this._getOverlayFit(l,e,i,a);if(d.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,c);return}if(this._canFitWithFlexibleDimensions(d,l,i)){o.push({position:a,origin:c,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(c,a)});continue}(!s||s.overlayFit.visibleArea<d.visibleArea)&&(s={overlayFit:d,overlayPoint:l,originPoint:c,position:a,overlayRect:e})}if(o.length){let a=null,c=-1;for(let l of o){let d=l.boundingBoxRect.width*l.boundingBoxRect.height*(l.position.weight||1);d>c&&(c=d,a=l)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&wo(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(rD),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof j?this._origin.nativeElement:dv(this._origin)?this._origin:null}_getOriginPoint(n,e,i){let r;if(i.originX=="center")r=n.left+n.width/2;else{let s=this._isRtl()?n.right:n.left,a=this._isRtl()?n.left:n.right;r=i.originX=="start"?s:a}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=n.top+n.height/2:o=i.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(n,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:n.x+r,y:n.y+o}}_getOverlayFit(n,e,i,r){let o=sD(e),{x:s,y:a}=n,c=this._getOffset(r,"x"),l=this._getOffset(r,"y");c&&(s+=c),l&&(a+=l);let d=0-s,f=s+o.width-i.width,m=0-a,g=a+o.height-i.height,b=this._subtractOverflows(o.width,d,f),I=this._subtractOverflows(o.height,m,g),N=b*I;return{visibleArea:N,isCompletelyWithinViewport:o.width*o.height===N,fitsInViewportVertically:I===o.height,fitsInViewportHorizontally:b==o.width}}_canFitWithFlexibleDimensions(n,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,s=oD(this._overlayRef.getConfig().minHeight),a=oD(this._overlayRef.getConfig().minWidth),c=n.fitsInViewportVertically||s!=null&&s<=r,l=n.fitsInViewportHorizontally||a!=null&&a<=o;return c&&l}return!1}_pushOverlayOnScreen(n,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let r=sD(e),o=this._viewportRect,s=Math.max(n.x+r.width-o.width,0),a=Math.max(n.y+r.height-o.height,0),c=Math.max(o.top-i.top-n.y,0),l=Math.max(o.left-i.left-n.x,0),d=0,f=0;return r.width<=o.width?d=l||-s:d=n.x<this._getViewportMarginStart()?o.left-i.left-n.x:0,r.height<=o.height?f=c||-a:f=n.y<this._getViewportMarginTop()?o.top-i.top-n.y:0,this._previousPushAmount={x:d,y:f},{x:n.x+d,y:n.y+f}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!U1(this._lastScrollVisibility,i)){let r=new gf(n,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=n.overlayY;n.overlayX==="center"?i="center":this._isRtl()?i=n.overlayX==="start"?"right":"left":i=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(n,e){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(e.overlayY==="top")s=n.y,o=i.height-s+this._getViewportMarginBottom();else if(e.overlayY==="bottom")a=i.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let g=Math.min(i.bottom-n.y+i.top,n.y),b=this._lastBoundingBoxSize.height;o=g*2,s=n.y-g,o>b&&!this._isInitialRender&&!this._growAfterOpen&&(s=n.y-b/2)}let c=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,l=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,d,f,m;if(l)m=i.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),d=n.x-this._getViewportMarginStart();else if(c)f=n.x,d=i.right-n.x-this._getViewportMarginEnd();else{let g=Math.min(i.right-n.x+i.left,n.x),b=this._lastBoundingBoxSize.width;d=g*2,f=n.x-g,d>b&&!this._isInitialRender&&!this._growAfterOpen&&(f=n.x-b/2)}return{top:s,left:f,bottom:a,right:m,width:d,height:o}}_setBoundingBoxStyles(n,e){let i=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=Xe(i.width),r.height=Xe(i.height),r.top=Xe(i.top)||"auto",r.bottom=Xe(i.bottom)||"auto",r.left=Xe(i.left)||"auto",r.right=Xe(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=Xe(o)),s&&(r.maxWidth=Xe(s))}this._lastBoundingBoxSize=i,wo(this._boundingBox.style,r)}_resetBoundingBoxStyles(){wo(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){wo(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let d=this._viewportRuler.getViewportScrollPosition();wo(i,this._getExactOverlayY(e,n,d)),wo(i,this._getExactOverlayX(e,n,d))}else i.position="static";let a="",c=this._getOffset(e,"x"),l=this._getOffset(e,"y");c&&(a+=`translateX(${c}px) `),l&&(a+=`translateY(${l}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=Xe(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=Xe(s.maxWidth):o&&(i.maxWidth="")),wo(this._pane.style,i)}_getExactOverlayY(n,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),n.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=Xe(o.y);return r}_getExactOverlayX(n,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=n.overlayX==="end"?"left":"right":s=n.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=Xe(o.x);return r}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:nD(n,i),isOriginOutsideView:cv(n,i),isOverlayClipped:nD(e,i),isOverlayOutsideView:cv(e,i)}}_subtractOverflows(n,...e){return e.reduce((i,r)=>i-Math.max(r,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+n-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&$g(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof j)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,i=n.height||0;return{top:n.y,bottom:n.y+i,left:n.x,right:n.x+e,height:i,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let i=e.getBoundingClientRect();return n&&(e.style.display=""),i}};function wo(t,n){for(let e in n)n.hasOwnProperty(e)&&(t[e]=n[e]);return t}function oD(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(B1);return!e||e==="px"?parseFloat(n):null}return t||null}function sD(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function U1(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var aD="cdk-global-overlay-wrapper";function gD(t){return new yf}var yf=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(aD),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,c=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),l=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),d=this._xPosition,f=this._xOffset,m=this._overlayRef.getConfig().direction==="rtl",g="",b="",I="";c?I="flex-start":d==="center"?(I="center",m?b=f:g=f):m?d==="left"||d==="end"?(I="flex-end",g=f):(d==="right"||d==="start")&&(I="flex-start",b=f):d==="left"||d==="start"?(I="flex-start",g=f):(d==="right"||d==="end")&&(I="flex-end",b=f),n.position=this._cssPosition,n.marginLeft=c?"0":g,n.marginTop=l?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=c?"0":b,e.justifyContent=I,e.alignItems=l?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(aD),i.justifyContent=i.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},vD=(()=>{class t{_injector=u(ae);global(){return gD()}flexibleConnectedTo(e){return jc(this._injector,e)}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),Bc=new y("OVERLAY_DEFAULT_CONFIG");function Uc(t,n){t.get(It).load(pD);let e=t.get(mD),i=t.get(U),r=t.get(mt),o=t.get(en),s=t.get(An),a=t.get(je,null,{optional:!0})||t.get(et).createRenderer(null,null),c=new Do(n),l=t.get(Bc,null,{optional:!0})?.usePopover??!0;c.direction=c.direction||s.value,"showPopover"in i.body?c.usePopover=n?.usePopover??l:c.usePopover=!1;let d=i.createElement("div"),f=i.createElement("div");d.id=r.getId("cdk-overlay-"),d.classList.add("cdk-overlay-pane"),f.appendChild(d),c.usePopover&&(f.setAttribute("popover","manual"),f.classList.add("cdk-overlay-popover"));let m=c.usePopover?c.positionStrategy?.getPopoverInsertionPoint?.():null;return dv(m)?m.after(f):m?.type==="parent"?m.element.appendChild(f):e.getContainerElement().appendChild(f),new vf(new Lc(d,o,t),f,d,c,t.get(O),t.get(fD),i,t.get(rr),t.get(hD),n?.disableAnimations??t.get(va,null,{optional:!0})==="NoopAnimations",t.get(xe),a)}var _D=(()=>{class t{scrollStrategies=u(dD);_positionBuilder=u(vD);_injector=u(ae);create(e){return Uc(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||t)};static \u0275prov=T({token:t,factory:t.\u0275fac})}return t})(),H1=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],z1=new y("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(ae);return()=>xo(t)}}),Ps=(()=>{class t{elementRef=u(j);static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return t})(),yD=new y("cdk-connected-overlay-default-config"),Cf=(()=>{class t{_dir=u(An,{optional:!0});_injector=u(ae);_overlayRef;_templatePortal;_backdropSubscription=fe.EMPTY;_attachSubscription=fe.EMPTY;_detachSubscription=fe.EMPTY;_positionSubscription=fe.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=u(z1);_ngZone=u(O);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new Y;positionChange=new Y;attach=new Y;detach=new Y;overlayKeydown=new Y;overlayOutsideClick=new Y;constructor(){let e=u(Jt),i=u(Rt),r=u(yD,{optional:!0}),o=u(Bc,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new Eo(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=H1);let e=this._overlayRef=Uc(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!Rn(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=xt(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new Do({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=jc(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof Ps?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof Ps?this.origin.elementRef.nativeElement:this.origin instanceof j?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(Xf(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",B],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",B],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",B],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",B],push:[2,"cdkConnectedOverlayPush","push",B],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",B],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",B],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[tt]})}return t})(),Hc=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({providers:[_D],imports:[Ie,eD,rv,rv]})}return t})();var bD=(()=>{class t{_animationsDisabled=St();state="unchecked";disabled=!1;appearance="full";static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&ce("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--mat-pseudo-checkbox-full-unselected-icon-color, var(--mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--mat-pseudo-checkbox-full-selected-icon-color, var(--mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-full-selected-checkmark-color, var(--mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2})}return t})();var $1=["text"],G1=[[["mat-icon"]],"*"],W1=["mat-icon","*"];function q1(t,n){if(t&1&&pe(0,"mat-pseudo-checkbox",1),t&2){let e=w();E("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function Y1(t,n){if(t&1&&pe(0,"mat-pseudo-checkbox",3),t&2){let e=w();E("disabled",e.disabled)}}function Z1(t,n){if(t&1&&(p(0,"span",4),_(1),h()),t&2){let e=w();v(),be("(",e.group.label,")")}}var fv=new y("MAT_OPTION_PARENT_COMPONENT"),hv=new y("MatOptgroup");var uv=class{source;isUserInput;constructor(n,e=!1){this.source=n,this.isUserInput=e}},br=(()=>{class t{_element=u(j);_changeDetectorRef=u(me);_parent=u(fv,{optional:!0});group=u(hv,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=u(mt).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=ee(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new Y;_text;_stateChanges=new S;constructor(){let e=u(It);e.load(Oi),e.load(Cc),this._signalDisableRipple=!!this._parent&&yn(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!Rn(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new uv(this,e))}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&Nt($1,7),i&2){let o;K(o=Q())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&F("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(wn("id",r.id),he("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),ce("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",B]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:W1,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(He(G1),_e(0,q1,1,2,"mat-pseudo-checkbox",1),ue(1),p(2,"span",2,0),ue(4,1),h(),_e(5,Y1,1,1,"mat-pseudo-checkbox",3),_e(6,Z1,2,1,"span",4),pe(7,"div",5)),i&2&&(ye(r.multiple?0:-1),v(5),ye(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),v(),ye(r.group&&r.group._inert?6:-1),v(),E("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[bD,nf],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--mat-option-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-option-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-option-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-option-label-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-option-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-option-label-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--mat-option-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--mat-option-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--mat-option-selected-state-layer-color, var(--mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --mat-list-list-item-selected-container-color: var(--mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})}return t})();function CD(t,n,e){if(e.length){let i=n.toArray(),r=e.toArray(),o=0;for(let s=0;s<t+1;s++)i[s].group&&i[s].group===r[o]&&o++;return o}return 0}function ED(t,n,e,i){return t<e?t:t+n>e+i?Math.max(0,t-i+n):e}var wD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({imports:[Ie]})}return t})();var pv=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({imports:[hr,wD,br,Ie]})}return t})();var K1=["trigger"],Q1=["panel"],X1=[[["mat-select-trigger"]],"*"],J1=["mat-select-trigger","*"];function eF(t,n){if(t&1&&(p(0,"span",4),_(1),h()),t&2){let e=w();v(),nt(e.placeholder)}}function tF(t,n){t&1&&ue(0)}function nF(t,n){if(t&1&&(p(0,"span",11),_(1),h()),t&2){let e=w(2);v(),nt(e.triggerValue)}}function iF(t,n){if(t&1&&(p(0,"span",5),_e(1,tF,1,0)(2,nF,2,1,"span",11),h()),t&2){let e=w();v(),ye(e.customTrigger?1:2)}}function rF(t,n){if(t&1){let e=Ue();p(0,"div",12,1),F("keydown",function(r){le(e);let o=w();return de(o._handleKeydown(r))}),ue(2,1),h()}if(t&2){let e=w();an(e.panelClass),ce("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",e._parentFormField?.color==="primary")("mat-accent",e._parentFormField?.color==="accent")("mat-warn",e._parentFormField?.color==="warn")("mat-undefined",!e._parentFormField?.color),he("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var oF=new y("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(ae);return()=>xo(t)}}),sF=new y("MAT_SELECT_CONFIG"),aF=new y("MatSelectTrigger"),mv=class{source;value;constructor(n,e){this.source=n,this.value=e}},Ef=(()=>{class t{_viewportRuler=u(yr);_changeDetectorRef=u(me);_elementRef=u(j);_dir=u(An,{optional:!0});_idGenerator=u(mt);_renderer=u(je);_parentFormField=u(Nc,{optional:!0});ngControl=u(ci,{self:!0,optional:!0});_liveAnnouncer=u(Wg);_defaultOptions=u(sF,{optional:!0});_animationsDisabled=St();_popoverLocation;_initialized=new S;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=CD(e,this.options,this.optionGroups),s=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=ED(s.offsetTop,s.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new mv(this,e)}_scrollStrategyFactory=u(oF);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new S;_errorStateTracker;stateChanges=new S;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=ee(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(po.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=kr(()=>{let e=this.options;return e?e.changes.pipe(_t(e),We(()=>Qt(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(We(()=>this.optionSelectionChanges))});openedChange=new Y;_openedStream=this.openedChange.pipe(Se(e=>e),ne(()=>{}));_closedStream=this.openedChange.pipe(Se(e=>!e),ne(()=>{}));selectionChange=new Y;valueChange=new Y;constructor(){let e=u(Rs),i=u(Zt,{optional:!0}),r=u(_c,{optional:!0}),o=u(new nr("tabindex"),{optional:!0}),s=u(Bc,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new Ns(e,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=s?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new Fc(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(ke(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(ke(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(_t(null),ke(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._panelOpen=!0,this._overlayDir.positionChange.pipe(Gt(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,s=this._keyManager;if(!s.isTyping()&&o&&!Rn(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let a=this.selected;s.onKeydown(e);let c=this.selected;c&&a!==c&&this._liveAnnouncer.announce(c.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,s=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!s&&(r===13||r===32)&&i.activeItem&&!Rn(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!s&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let a=this.options.some(c=>!c.disabled&&!c.selected);this.options.forEach(c=>{c.disabled||(a?c.select():c.deselect())})}else{let a=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==a&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!Rn(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch(o){return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof Ps?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new Ic(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=Qt(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(ke(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),Qt(...this.options.map(i=>i._stateChanges)).pipe(ke(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=xt(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&Dn(o,aF,5)(o,br,5)(o,hv,5),i&2){let s;K(s=Q())&&(r.customTrigger=s.first),K(s=Q())&&(r.options=s),K(s=Q())&&(r.optionGroups=s)}},viewQuery:function(i,r){if(i&1&&Nt(K1,5)(Q1,5)(Cf,5),i&2){let o;K(o=Q())&&(r.trigger=o.first),K(o=Q())&&(r.panel=o.first),K(o=Q())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&F("keydown",function(s){return r._handleKeydown(s)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(he("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),ce("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",B],disableRipple:[2,"disableRipple","disableRipple",B],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:xn(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",B],placeholder:"placeholder",required:[2,"required","required",B],multiple:[2,"multiple","multiple",B],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",B],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",xn],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",B]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[ze([{provide:Rc,useExisting:t},{provide:fv,useExisting:t}]),tt],ngContentSelectors:J1,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(He(X1),p(0,"div",2,0),F("click",function(){return r.open()}),p(3,"div",3),_e(4,eF,2,1,"span",4)(5,iF,3,1,"span",5),h(),p(6,"div",6)(7,"div",7),ei(),p(8,"svg",8),pe(9,"path",9),h()()()(),$(10,rF,3,16,"ng-template",10),F("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(s){return r._handleOverlayKeydown(s)})),i&2){let o=ri(1);v(3),he("id",r._valueId),v(),ye(r.empty?4:5),v(6),E("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[Ps,Cf],styles:[`@keyframes _mat-select-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-select-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-select {
  display: inline-block;
  width: 100%;
  outline: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-select-enabled-trigger-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-select-trigger-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-select-trigger-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-select-trigger-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-select-trigger-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-select-trigger-text-tracking, var(--mat-sys-body-large-tracking));
}

div.mat-mdc-select-panel {
  box-shadow: var(--mat-select-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}

.mat-mdc-select-disabled {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-select-disabled .mat-mdc-select-placeholder {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-select-trigger {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  box-sizing: border-box;
  width: 100%;
}
.mat-mdc-select-disabled .mat-mdc-select-trigger {
  -webkit-user-select: none;
  user-select: none;
  cursor: default;
}

.mat-mdc-select-value {
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mat-mdc-select-value-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mat-mdc-select-arrow-wrapper {
  height: 24px;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
}
.mat-form-field-appearance-fill .mdc-text-field--no-label .mat-mdc-select-arrow-wrapper {
  transform: none;
}

.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-invalid .mat-mdc-select-arrow,
.mat-form-field-invalid:not(.mat-form-field-disabled) .mat-mdc-form-field-infix::after {
  color: var(--mat-select-invalid-arrow-color, var(--mat-sys-error));
}

.mat-mdc-select-arrow {
  width: 10px;
  height: 5px;
  position: relative;
  color: var(--mat-select-enabled-arrow-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field.mat-focused .mat-mdc-select-arrow {
  color: var(--mat-select-focused-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-disabled .mat-mdc-select-arrow {
  color: var(--mat-select-disabled-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-select-open .mat-mdc-select-arrow {
  transform: rotate(180deg);
}
.mat-form-field-animations-enabled .mat-mdc-select-arrow {
  transition: transform 80ms linear;
}
.mat-mdc-select-arrow svg {
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}
@media (forced-colors: active) {
  .mat-mdc-select-arrow svg {
    fill: CanvasText;
  }
  .mat-mdc-select-disabled .mat-mdc-select-arrow svg {
    fill: GrayText;
  }
}

div.mat-mdc-select-panel {
  width: 100%;
  max-height: 275px;
  outline: 0;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  transform-origin: top center;
  border-radius: 0 0 4px 4px;
  position: relative;
  background-color: var(--mat-select-panel-background-color, var(--mat-sys-surface-container));
}
.mat-mdc-select-panel-above div.mat-mdc-select-panel {
  border-radius: 4px 4px 0 0;
  transform-origin: bottom center;
}
@media (forced-colors: active) {
  div.mat-mdc-select-panel {
    outline: solid 1px;
  }
}

.mat-select-panel-animations-enabled {
  animation: _mat-select-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-select-panel-animations-enabled.mat-select-panel-exit {
  animation: _mat-select-exit 100ms linear;
}

.mat-mdc-select-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  color: var(--mat-select-placeholder-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field:not(.mat-form-field-animations-enabled) .mat-mdc-select-placeholder, ._mat-animation-noopable .mat-mdc-select-placeholder {
  transition: none;
}
.mat-form-field-hide-placeholder .mat-mdc-select-placeholder {
  color: transparent;
  -webkit-text-fill-color: transparent;
  transition: none;
  display: block;
}

.mat-mdc-form-field-type-mat-select:not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper {
  cursor: pointer;
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mat-mdc-floating-label {
  max-width: calc(100% - 18px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mdc-floating-label--float-above {
  max-width: calc(100% / 0.75 - 24px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-notched-outline__notch {
  max-width: calc(100% - 60px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-text-field--label-floating .mdc-notched-outline__notch {
  max-width: calc(100% - 24px);
}

.mat-mdc-select-min-line:empty::before {
  content: " ";
  white-space: pre;
  width: 1px;
  display: inline-block;
  visibility: hidden;
}

.mat-form-field-appearance-fill .mat-mdc-select-arrow-wrapper {
  transform: var(--mat-select-arrow-transform, translateY(-8px));
}
`],encapsulation:2})}return t})();var Ls=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({imports:[Hc,pv,Ie,Fs,Je,pv]})}return t})();function cF(t,n){t&1&&(p(0,"div",3),pe(1,"mat-spinner",4),h())}function lF(t,n){if(t&1&&(p(0,"mat-option",17),_(1),h()),t&2){let e=n.$implicit;E("value",e),v(),be(" ",e," ")}}function dF(t,n){if(t&1&&(p(0,"div",18),_(1),h()),t&2){let e=w(2);v(),be(" ",e.mensagens," ")}}function uF(t,n){if(t&1){let e=Ue();p(0,"form",5),F("ngSubmit",function(){le(e);let r=w();return de(r.onSubmit())})("keydown.enter",function(r){le(e);let o=w();return r.preventDefault(),de(o.onSubmit())}),p(1,"mat-form-field",6)(2,"mat-label"),_(3,"Nome"),h(),p(4,"input",7),Ne("ngModelChange",function(r){le(e);let o=w();return Pe(o.usuario.nome,r)||(o.usuario.nome=r),de(r)}),h(),Oe(),p(5,"mat-icon",8),_(6,"person"),h()(),p(7,"mat-form-field",6)(8,"mat-label"),_(9,"Email"),h(),p(10,"input",9),Ne("ngModelChange",function(r){le(e);let o=w();return Pe(o.usuario.email,r)||(o.usuario.email=r),de(r)}),h(),Oe(),p(11,"mat-icon",8),_(12,"email"),h()(),p(13,"mat-form-field",6)(14,"mat-label"),_(15,"Nova senha"),h(),p(16,"input",10),Ne("ngModelChange",function(r){le(e);let o=w();return Pe(o.usuario.senha,r)||(o.usuario.senha=r),de(r)}),h(),Oe(),p(17,"mat-icon",8),_(18,"lock"),h(),p(19,"mat-hint"),_(20," Deixe em branco para manter a senha atual. "),h()(),p(21,"mat-form-field",6)(22,"mat-label"),_(23,"Perfil"),h(),p(24,"mat-select",11),Ne("ngModelChange",function(r){le(e);let o=w();return Pe(o.usuario.role,r)||(o.usuario.role=r),de(r)}),$(25,lF,2,2,"mat-option",12),h(),Oe(),p(26,"mat-icon",8),_(27,"admin_panel_settings"),h()(),$(28,dF,2,1,"div",13),p(29,"div",14)(30,"button",15)(31,"mat-icon"),_(32,"save"),h(),_(33," Salvar "),h(),p(34,"button",16)(35,"mat-icon"),_(36,"arrow_back"),h(),_(37," Voltar "),h()()()}if(t&2){let e=w();v(4),Re("ngModel",e.usuario.nome),Fe(),v(6),Re("ngModel",e.usuario.email),Fe(),v(6),Re("ngModel",e.usuario.senha),Fe(),v(8),Re("ngModel",e.usuario.role),Fe(),v(),E("ngForOf",e.roles),v(3),E("ngIf",e.mensagens)}}var wf=class t{constructor(n,e,i,r,o){this.usuarioService=n;this.authService=e;this.route=i;this.router=r;this.cdr=o}usuarioService;authService;route;router;cdr;roles=["USER","ADMIN"];usuario={nome:"",email:"",senha:"",role:"USER"};carregando=!0;mensagens="";autoEdicao=!1;ngOnInit(){let n=this.route.snapshot.paramMap.get("id"),e=localStorage.getItem("id");console.log("idParam:",n),console.log("idLogado:",e);let i=localStorage.getItem("role")==="USER"||!n?e:n;console.log("id alvo:",i),i?this.usuarioService.porId(Number(i)).subscribe({next:r=>{this.usuario=H(C({},r),{senha:""}),this.autoEdicao=e!=null&&String(r.id)===e,this.carregando=!1,this.cdr.detectChanges()},error:r=>{console.error("Erro ao carregar usuario",r),this.mensagens="Usuario nao encontrado.",this.carregando=!1,this.cdr.detectChanges()}}):this.carregando=!1}onSubmit(){let n=C({},this.usuario);n.senha||delete n.senha,this.usuarioService.atualizar(n).subscribe({next:()=>{if(this.autoEdicao){this.authService.logout(),this.router.navigate(["/login"]);return}this.router.navigate(["/usuarios"])},error:e=>{console.error("Erro ao salvar usuario",e),this.mensagens=e.error?.erro??e.status===403?"Sem permissao para essa acao.":"Nao foi possivel salvar o usuario.",this.cdr.detectChanges()}}),this.cdr.detectChanges()}static \u0275fac=function(e){return new(e||t)(x(_r),x(kn),x(Yt),x(Ve),x(me))};static \u0275cmp=R({type:t,selectors:[["app-usuario-form"]],decls:6,vars:2,consts:[[1,"page"],["class","loading",4,"ngIf"],[3,"ngSubmit","keydown.enter",4,"ngIf"],[1,"loading"],["diameter","45"],[3,"ngSubmit","keydown.enter"],["appearance","outline"],["matInput","","id","nome","name","nome","required","",3,"ngModelChange","ngModel"],["matSuffix",""],["matInput","","type","email","id","email","name","email","required","",3,"ngModelChange","ngModel"],["matInput","","type","password","id","senha","name","senha",3,"ngModelChange","ngModel"],["id","role","name","role",3,"ngModelChange","ngModel"],[3,"value",4,"ngFor","ngForOf"],["class","success",4,"ngIf"],[1,"actions"],["mat-raised-button","","color","primary","type","submit"],["mat-stroked-button","","routerLink","/usuarios","type","button"],[3,"value"],[1,"success"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card")(2,"h2"),_(3,"\u{1F464} Editar Usu\xE1rio"),h(),$(4,cF,2,0,"div",1)(5,uF,38,6,"form",2),h()()),e&2&&(v(4),E("ngIf",i.carregando),v(),E("ngIf",!i.carregando))},dependencies:[it,tn,ct,Ft,di,pt,Ot,li,Tn,Dt,Zt,rt,Lt,Pt,Je,vt,lt,Ac,fn,Bt,jt,Ls,Ef,br,gt,Vt,Ht,Ut,Fn,On],encapsulation:2})};var Vs=class t{constructor(n){this.http=n}http;listar(){return this.http.get(`${Ge.apiUrl}/pet/all`)}porId(n){return this.http.get(`${Ge.apiUrl}/pet/${n}`)}criar(n){return this.http.post(`${Ge.apiUrl}/pet`,n)}atualizar(n){return this.http.put(`${Ge.apiUrl}/pet`,n)}remover(n){return this.http.delete(`${Ge.apiUrl}/pet/${n}`)}static \u0275fac=function(e){return new(e||t)(M(Sn))};static \u0275prov=V({token:t,factory:t.\u0275fac,providedIn:"root"})};var fF=t=>["/pets",t];function hF(t,n){if(t&1){let e=Ue();p(0,"button",11),F("click",function(){le(e);let r=w();return r.termoBusca="",de(r.buscarPets())}),p(1,"mat-icon"),_(2,"close"),h()()}}function pF(t,n){if(t&1&&(p(0,"div",12),_(1),h()),t&2){let e=w();v(),be(" ",e.mensagens," ")}}function mF(t,n){t&1&&(p(0,"div",13),pe(1,"mat-spinner",14),h())}function gF(t,n){if(t&1){let e=Ue();p(0,"tr")(1,"td"),_(2),h(),p(3,"td"),_(4),h(),p(5,"td"),_(6),h(),p(7,"td",18)(8,"button",19)(9,"mat-icon"),_(10,"edit"),h()(),p(11,"button",20),F("click",function(){let r=le(e).$implicit,o=w(2);return de(o.remover(r.id))}),p(12,"mat-icon"),_(13,"delete"),h()()()()}if(t&2){let e=n.$implicit;v(2),nt(e.nome),v(2),nt(e.especie),v(2),nt(e.dono?.email??"-"),v(2),E("routerLink",oo(4,fF,e.id))}}function vF(t,n){if(t&1&&(p(0,"table",15)(1,"thead")(2,"tr")(3,"th"),_(4,"Nome"),h(),p(5,"th"),_(6,"Esp\xE9cie"),h(),p(7,"th"),_(8,"Dono"),h(),p(9,"th",16),_(10,"A\xE7\xF5es"),h()()(),p(11,"tbody"),$(12,gF,14,6,"tr",17),h()()),t&2){let e=w();v(12),E("ngForOf",e.petsBuscados)}}function _F(t,n){t&1&&(p(0,"p"),_(1,"Clique em "),p(2,"strong"),_(3,"Novo Pet"),h(),_(4," para come\xE7ar."),h())}function yF(t,n){t&1&&(p(0,"p"),_(1,"Tente outro termo de busca."),h())}function bF(t,n){if(t&1&&(p(0,"div",21)(1,"mat-icon",22),_(2,"pets"),h(),p(3,"h3"),_(4),h(),$(5,_F,5,0,"p",23)(6,yF,2,0,"p",23),h()),t&2){let e=w();v(4),nt(e.termoBusca?"Nenhum pet encontrado":"Nenhum pet cadastrado"),v(),E("ngIf",!e.termoBusca),v(),E("ngIf",e.termoBusca)}}var Df=class t{constructor(n,e){this.petService=n;this.cdr=e}petService;cdr;pets=[];carregando=!0;mensagens="";termoBusca="";petsBuscados=[];ngOnInit(){this.carregar()}buscarPets(){let n=this.termoBusca.trim().toLowerCase();if(!n){this.petsBuscados=this.pets;return}this.petsBuscados=this.pets.filter(e=>e.nome?.toLowerCase().includes(n)||e.especie?.toLowerCase().includes(n)||e.dono?.email?.toLowerCase().includes(n))}carregar(){this.carregando=!0,this.petService.listar().subscribe({next:n=>{this.pets=n,this.buscarPets(),this.carregando=!1,this.cdr.detectChanges()},error:n=>{this.mensagens="Nao foi possivel carregar os pets.",this.carregando=!1,this.cdr.detectChanges()}})}remover(n){n!=null&&confirm("Confirma\xE7ao para remover")&&this.petService.remover(n).subscribe({next:()=>{this.pets=this.pets.filter(e=>e.id!==n),this.buscarPets(),this.cdr.detectChanges()},error:e=>{console.error("Erro ao remover pet",e),this.mensagens="Nao foi possivel remover este pet.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(x(Vs),x(me))};static \u0275cmp=R({type:t,selectors:[["app-gestao-pets"]],decls:20,vars:6,consts:[[1,"page"],[1,"header"],["mat-raised-button","","color","primary","routerLink","/pets/novo"],["appearance","outline",1,"search-field"],["matInput","","placeholder","Nome, esp\xE9cie ou dono...",3,"ngModelChange","ngModel"],["matPrefix",""],["mat-icon-button","","matSuffix","",3,"click",4,"ngIf"],["class","error",4,"ngIf"],["class","loading",4,"ngIf"],["class","mat-elevation-z2",4,"ngIf"],["class","empty-state",4,"ngIf"],["mat-icon-button","","matSuffix","",3,"click"],[1,"error"],[1,"loading"],["diameter","45"],[1,"mat-elevation-z2"],[2,"width","170px"],[4,"ngFor","ngForOf"],[1,"actions"],["mat-icon-button","","color","primary",3,"routerLink"],["mat-icon-button","","color","warn",3,"click"],[1,"empty-state"],[1,"empty-icon"],[4,"ngIf"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card")(2,"div",1)(3,"h2"),_(4,"\u{1F43E} Gest\xE3o de Pets"),h(),p(5,"button",2)(6,"mat-icon"),_(7,"add"),h(),_(8," Novo Pet "),h()(),p(9,"mat-form-field",3)(10,"mat-label"),_(11,"Pesquisar"),h(),p(12,"input",4),Ne("ngModelChange",function(o){return Pe(i.termoBusca,o)||(i.termoBusca=o),o}),F("ngModelChange",function(){return i.buscarPets()}),h(),Oe(),p(13,"mat-icon",5),_(14,"search"),h(),$(15,hF,3,0,"button",6),h(),$(16,pF,2,1,"div",7)(17,mF,2,0,"div",8)(18,vF,13,1,"table",9)(19,bF,7,3,"div",10),h()()),e&2&&(v(12),Re("ngModel",i.termoBusca),Fe(),v(3),E("ngIf",i.termoBusca),v(),E("ngIf",i.mensagens),v(),E("ngIf",i.carregando),v(),E("ngIf",!i.carregando&&i.petsBuscados.length>0),v(),E("ngIf",!i.carregando&&i.petsBuscados.length===0))},dependencies:[it,tn,ct,rt,Lt,Pt,gt,Vt,Fi,Ht,Ut,Fn,On,Je,vt,lt,Co,fn,Bt,jt,Ft,pt,Ot,Dt],encapsulation:2})};function CF(t,n){t&1&&(p(0,"div",3),pe(1,"mat-spinner",4),h())}function EF(t,n){if(t&1&&(p(0,"mat-option",15),_(1),h()),t&2){let e=n.$implicit;E("value",e),v(),be(" ",e," ")}}function wF(t,n){if(t&1&&(p(0,"div",16),_(1),h()),t&2){let e=w(2);v(),be(" ",e.mensagens," ")}}function DF(t,n){if(t&1){let e=Ue();p(0,"form",5),F("ngSubmit",function(){le(e);let r=w();return de(r.onSubmit())})("keydown.enter",function(r){le(e);let o=w();return r.preventDefault(),de(o.onSubmit())}),p(1,"mat-form-field",6)(2,"mat-label"),_(3,"Nome"),h(),p(4,"input",7),Ne("ngModelChange",function(r){le(e);let o=w();return Pe(o.pet.nome,r)||(o.pet.nome=r),de(r)}),h(),Oe(),p(5,"mat-icon",8),_(6,"pets"),h()(),p(7,"mat-form-field",6)(8,"mat-label"),_(9,"Esp\xE9cie"),h(),p(10,"mat-select",9),Ne("ngModelChange",function(r){le(e);let o=w();return Pe(o.pet.especie,r)||(o.pet.especie=r),de(r)}),$(11,EF,2,2,"mat-option",10),h(),Oe(),p(12,"mat-icon",8),_(13,"category"),h()(),$(14,wF,2,1,"div",11),p(15,"div",12)(16,"button",13)(17,"mat-icon"),_(18,"save"),h(),_(19," Salvar "),h(),p(20,"button",14)(21,"mat-icon"),_(22,"arrow_back"),h(),_(23," Voltar "),h()()()}if(t&2){let e=w();v(4),Re("ngModel",e.pet.nome),Fe(),v(6),Re("ngModel",e.pet.especie),Fe(),v(),E("ngForOf",e.especies),v(3),E("ngIf",e.mensagens)}}var zc=class t{constructor(n,e,i,r){this.petService=n;this.route=e;this.router=i;this.cdr=r}petService;route;router;cdr;especies=["CACHORRO","GATO","PEIXE","ROEDOR","AVE","OUTRA"];pet={nome:"",especie:""};modoEdicao=!1;carregando=!1;mensagens="";ngOnInit(){let n=this.route.snapshot.paramMap.get("id");n&&(this.modoEdicao=!0,this.carregando=!0,this.petService.porId(Number(n)).subscribe({next:e=>{this.pet=e,this.carregando=!1,this.cdr.detectChanges()},error:e=>{console.error("Erro ao carregar pet",e),this.mensagens="Pet nao encontrado.",this.carregando=!1,this.cdr.detectChanges()}}))}onSubmit(){(this.modoEdicao?this.petService.atualizar(this.pet):this.petService.criar(this.pet)).subscribe({next:()=>{this.router.navigate(["/pets"])},error:e=>{console.error("Erro ao salvar pet",e),this.mensagens=e.error?.erro??e.status===403?"Sem permissao para essa acao.":"Nao foi possivel salvar o pet.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(x(Vs),x(Yt),x(Ve),x(me))};static \u0275cmp=R({type:t,selectors:[["app-pet-form"]],decls:6,vars:3,consts:[[1,"page"],["class","loading",4,"ngIf"],[3,"ngSubmit","keydown.enter",4,"ngIf"],[1,"loading"],["diameter","45"],[3,"ngSubmit","keydown.enter"],["appearance","outline"],["matInput","","id","nome","name","nome","required","",3,"ngModelChange","ngModel"],["matSuffix",""],["id","especie","name","especie","required","",3,"ngModelChange","ngModel"],[3,"value",4,"ngFor","ngForOf"],["class","error",4,"ngIf"],[1,"actions"],["mat-raised-button","","color","primary","type","submit"],["mat-stroked-button","","routerLink","/pets","type","button"],[3,"value"],[1,"error"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card")(2,"h2"),_(3),h(),$(4,CF,2,0,"div",1)(5,DF,24,4,"form",2),h()()),e&2&&(v(3),be(" ",i.modoEdicao?"\u270F\uFE0F Editar Pet":"\u{1F43E} Novo Pet"," "),v(),E("ngIf",i.carregando),v(),E("ngIf",!i.carregando))},dependencies:[it,tn,ct,Ft,di,pt,Ot,li,Tn,Dt,Zt,rt,Lt,Pt,Je,vt,lt,fn,Bt,jt,Ls,Ef,br,gt,Vt,Ht,Ut,Fn,On],styles:[".actions[_ngcontent-%COMP%]{display:flex;gap:12px;margin-top:20px}.actions[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{min-width:140px}"]})};var js=()=>{let t=u(Ve);return localStorage.getItem("token")?!0:(t.navigate(["/login"]),!1)};var xf=()=>{let t=u(Ve),n=localStorage.getItem("token"),e=localStorage.getItem("role");return n&&e==="ADMIN"?!0:(t.navigate(["/pets"]),!1)};var gv=()=>(localStorage.clear(),!0);var xF=[[["mat-icon"],["","matMenuItemIcon",""]],"*"],IF=["mat-icon, [matMenuItemIcon]","*"];function SF(t,n){t&1&&(ei(),p(0,"svg",2),pe(1,"polygon",3),h())}var MF=["*"];function TF(t,n){if(t&1){let e=Ue();ft(0,"div",0),zd("click",function(){le(e);let r=w();return de(r.closed.emit("click"))})("animationstart",function(r){le(e);let o=w();return de(o._onAnimationStart(r.animationName))})("animationend",function(r){le(e);let o=w();return de(o._onAnimationDone(r.animationName))})("animationcancel",function(r){le(e);let o=w();return de(o._onAnimationDone(r.animationName))}),ft(1,"div",1),ue(2),wt()()}if(t&2){let e=w();an(e._classList),ce("mat-menu-panel-animations-disabled",e._animationsDisabled)("mat-menu-panel-exit-animation",e._panelAnimationState==="void")("mat-menu-panel-animating",e._isAnimating()),wn("id",e.panelId),he("aria-label",e.ariaLabel||null)("aria-labelledby",e.ariaLabelledby||null)("aria-describedby",e.ariaDescribedby||null)}}var _v=new y("MAT_MENU_PANEL"),$c=(()=>{class t{_elementRef=u(j);_document=u(U);_focusMonitor=u(fr);_parentMenu=u(_v,{optional:!0});_changeDetectorRef=u(me);role="menuitem";disabled=!1;disableRipple=!1;_hovered=new S;_focused=new S;_highlighted=!1;_triggersSubmenu=!1;constructor(){u(It).load(Oi),this._parentMenu?.addItem?.(this)}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._getHostElement(),e,i):this._getHostElement().focus(i),this._focused.next(this)}ngAfterViewInit(){this._focusMonitor&&this._focusMonitor.monitor(this._elementRef,!1)}ngOnDestroy(){this._focusMonitor&&this._focusMonitor.stopMonitoring(this._elementRef),this._parentMenu&&this._parentMenu.removeItem&&this._parentMenu.removeItem(this),this._hovered.complete(),this._focused.complete()}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._elementRef.nativeElement}_checkDisabled(e){this.disabled&&(e.preventDefault(),e.stopPropagation())}_handleMouseEnter(){this._hovered.next(this)}getLabel(){let e=this._elementRef.nativeElement.cloneNode(!0),i=e.querySelectorAll("mat-icon, .material-icons");for(let r=0;r<i.length;r++)i[r].remove();return e.textContent?.trim()||""}_setHighlighted(e){this._highlighted=e,this._changeDetectorRef.markForCheck()}_setTriggersSubmenu(e){this._triggersSubmenu=e,this._changeDetectorRef.markForCheck()}_hasFocus(){return this._document&&this._document.activeElement===this._getHostElement()}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["","mat-menu-item",""]],hostAttrs:[1,"mat-mdc-menu-item","mat-focus-indicator"],hostVars:8,hostBindings:function(i,r){i&1&&F("click",function(s){return r._checkDisabled(s)})("mouseenter",function(){return r._handleMouseEnter()}),i&2&&(he("role",r.role)("tabindex",r._getTabIndex())("aria-disabled",r.disabled)("disabled",r.disabled||null),ce("mat-mdc-menu-item-highlighted",r._highlighted)("mat-mdc-menu-item-submenu-trigger",r._triggersSubmenu))},inputs:{role:"role",disabled:[2,"disabled","disabled",B],disableRipple:[2,"disableRipple","disableRipple",B]},exportAs:["matMenuItem"],ngContentSelectors:IF,decls:5,vars:3,consts:[[1,"mat-mdc-menu-item-text"],["matRipple","",1,"mat-mdc-menu-ripple",3,"matRippleDisabled","matRippleTrigger"],["viewBox","0 0 5 10","focusable","false","aria-hidden","true",1,"mat-mdc-menu-submenu-icon"],["points","0,0 5,5 0,10"]],template:function(i,r){i&1&&(He(xF),ue(0),p(1,"span",0),ue(2,1),h(),pe(3,"div",1),_e(4,SF,2,0,":svg:svg",2)),i&2&&(v(3),E("matRippleDisabled",r.disableRipple||r.disabled)("matRippleTrigger",r._getHostElement()),v(),ye(r._triggersSubmenu?4:-1))},dependencies:[nf],encapsulation:2})}return t})();var AF=new y("MatMenuContent");var RF=new y("mat-menu-default-options",{providedIn:"root",factory:()=>({overlapTrigger:!1,xPosition:"after",yPosition:"below",backdropClass:"cdk-overlay-transparent-backdrop"})}),vv="_mat-menu-enter",If="_mat-menu-exit",Us=(()=>{class t{_elementRef=u(j);_changeDetectorRef=u(me);_injector=u(ae);_keyManager;_xPosition;_yPosition;_firstItemFocusRef;_exitFallbackTimeout;_animationsDisabled=St();_allItems;_directDescendantItems=new zn;_classList={};_panelAnimationState="void";_animationDone=new S;_isAnimating=ee(!1);parentMenu;direction;overlayPanelClass;backdropClass;ariaLabel;ariaLabelledby;ariaDescribedby;get xPosition(){return this._xPosition}set xPosition(e){this._xPosition=e,this.setPositionClasses()}get yPosition(){return this._yPosition}set yPosition(e){this._yPosition=e,this.setPositionClasses()}templateRef;items;lazyContent;overlapTrigger=!1;hasBackdrop;get panelClass(){return this._previousPanelClass}set panelClass(e){let i=this._previousPanelClass,r=C({},this._classList);i&&i.length&&i.split(" ").forEach(o=>{r[o]=!1}),this._previousPanelClass=e,e&&e.length&&(e.split(" ").forEach(o=>{r[o]=!0}),this._elementRef.nativeElement.className=""),this._classList=r}_previousPanelClass="";get classList(){return this.panelClass}set classList(e){this.panelClass=e}closed=new Y;close=this.closed;panelId=u(mt).getId("mat-menu-panel-");constructor(){let e=u(RF);this.overlayPanelClass=e.overlayPanelClass||"",this._xPosition=e.xPosition,this._yPosition=e.yPosition,this.backdropClass=e.backdropClass,this.overlapTrigger=e.overlapTrigger,this.hasBackdrop=e.hasBackdrop}ngOnInit(){this.setPositionClasses()}ngAfterContentInit(){this._updateDirectDescendants(),this._keyManager=new yo(this._directDescendantItems).withWrap().withTypeAhead().withHomeAndEnd(),this._keyManager.tabOut.subscribe(()=>this.closed.emit("tab")),this._directDescendantItems.changes.pipe(_t(this._directDescendantItems),We(e=>Qt(...e.map(i=>i._focused)))).subscribe(e=>this._keyManager.updateActiveItem(e)),this._directDescendantItems.changes.subscribe(e=>{let i=this._keyManager;if(this._panelAnimationState==="enter"&&i.activeItem?._hasFocus()){let r=e.toArray(),o=Math.max(0,Math.min(r.length-1,i.activeItemIndex||0));r[o]&&!r[o].disabled?i.setActiveItem(o):i.setNextItemActive()}})}ngOnDestroy(){this._keyManager?.destroy(),this._directDescendantItems.destroy(),this.closed.complete(),this._firstItemFocusRef?.destroy(),clearTimeout(this._exitFallbackTimeout)}_hovered(){return this._directDescendantItems.changes.pipe(_t(this._directDescendantItems),We(i=>Qt(...i.map(r=>r._hovered))))}addItem(e){}removeItem(e){}_handleKeydown(e){let i=e.keyCode,r=this._keyManager;switch(i){case 27:Rn(e)||(e.preventDefault(),this.closed.emit("keydown"));break;case 37:this.parentMenu&&this.direction==="ltr"&&this.closed.emit("keydown");break;case 39:this.parentMenu&&this.direction==="rtl"&&this.closed.emit("keydown");break;default:(i===38||i===40)&&r.setFocusOrigin("keyboard"),r.onKeydown(e);return}}focusFirstItem(e="program"){this._firstItemFocusRef?.destroy(),this._firstItemFocusRef=Cn(()=>{let i=this._resolvePanel();if(!i||!i.contains(document.activeElement)){let r=this._keyManager;r.setFocusOrigin(e).setFirstItemActive(),!r.activeItem&&i&&i.focus()}},{injector:this._injector})}resetActiveItem(){this._keyManager.setActiveItem(-1)}setElevation(e){}setPositionClasses(e=this.xPosition,i=this.yPosition){this._classList=H(C({},this._classList),{"mat-menu-before":e==="before","mat-menu-after":e==="after","mat-menu-above":i==="above","mat-menu-below":i==="below"}),this._changeDetectorRef.markForCheck()}_onAnimationDone(e){let i=e===If;(i||e===vv)&&(i&&(clearTimeout(this._exitFallbackTimeout),this._exitFallbackTimeout=void 0),this._animationDone.next(i?"void":"enter"),this._isAnimating.set(!1))}_onAnimationStart(e){(e===vv||e===If)&&this._isAnimating.set(!0)}_setIsOpen(e){if(this._panelAnimationState=e?"enter":"void",e){if(this._keyManager.activeItemIndex===0){let i=this._resolvePanel();i&&(i.scrollTop=0)}}else this._animationsDisabled||(this._exitFallbackTimeout=setTimeout(()=>this._onAnimationDone(If),200));this._animationsDisabled&&setTimeout(()=>{this._onAnimationDone(e?vv:If)}),this._changeDetectorRef.markForCheck()}_updateDirectDescendants(){this._allItems.changes.pipe(_t(this._allItems)).subscribe(e=>{this._directDescendantItems.reset(e.filter(i=>i._parentMenu===this)),this._directDescendantItems.notifyOnChanges()})}_resolvePanel(){let e=null;return this._directDescendantItems.length&&(e=this._directDescendantItems.first._getHostElement().closest('[role="menu"]')),e}static \u0275fac=function(i){return new(i||t)};static \u0275cmp=R({type:t,selectors:[["mat-menu"]],contentQueries:function(i,r,o){if(i&1&&Dn(o,AF,5)(o,$c,5)(o,$c,4),i&2){let s;K(s=Q())&&(r.lazyContent=s.first),K(s=Q())&&(r._allItems=s),K(s=Q())&&(r.items=s)}},viewQuery:function(i,r){if(i&1&&Nt(Jt,5),i&2){let o;K(o=Q())&&(r.templateRef=o.first)}},hostVars:3,hostBindings:function(i,r){i&2&&he("aria-label",null)("aria-labelledby",null)("aria-describedby",null)},inputs:{backdropClass:"backdropClass",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],xPosition:"xPosition",yPosition:"yPosition",overlapTrigger:[2,"overlapTrigger","overlapTrigger",B],hasBackdrop:[2,"hasBackdrop","hasBackdrop",e=>e==null?null:B(e)],panelClass:[0,"class","panelClass"],classList:"classList"},outputs:{closed:"closed",close:"close"},exportAs:["matMenu"],features:[ze([{provide:_v,useExisting:t}])],ngContentSelectors:MF,decls:1,vars:0,consts:[["tabindex","-1","role","menu",1,"mat-mdc-menu-panel",3,"click","animationstart","animationend","animationcancel","id"],[1,"mat-mdc-menu-content"]],template:function(i,r){i&1&&(He(),Ud(0,TF,3,12,"ng-template"))},styles:[`mat-menu {
  display: none;
}

.mat-mdc-menu-content {
  margin: 0;
  padding: 8px 0;
  outline: 0;
}
.mat-mdc-menu-content,
.mat-mdc-menu-content .mat-mdc-menu-item .mat-mdc-menu-item-text {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  flex: 1;
  white-space: normal;
  font-family: var(--mat-menu-item-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-menu-item-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-menu-item-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-menu-item-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-menu-item-label-text-weight, var(--mat-sys-label-large-weight));
}

@keyframes _mat-menu-enter {
  from {
    opacity: 0;
    transform: scale(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-menu-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-menu-panel {
  min-width: 112px;
  max-width: 280px;
  overflow: auto;
  box-sizing: border-box;
  outline: 0;
  animation: _mat-menu-enter 120ms cubic-bezier(0, 0, 0.2, 1);
  border-radius: var(--mat-menu-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-menu-container-color, var(--mat-sys-surface-container));
  box-shadow: var(--mat-menu-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
  will-change: transform, opacity;
}
.mat-mdc-menu-panel.mat-menu-panel-exit-animation {
  animation: _mat-menu-exit 100ms 25ms linear forwards;
}
.mat-mdc-menu-panel.mat-menu-panel-animations-disabled {
  animation: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating {
  pointer-events: none;
}
.mat-mdc-menu-panel.mat-menu-panel-animating:has(.mat-mdc-menu-content:empty) {
  display: none;
}
@media (forced-colors: active) {
  .mat-mdc-menu-panel {
    outline: solid 1px;
  }
}
.mat-mdc-menu-panel .mat-divider {
  border-top-color: var(--mat-menu-divider-color, var(--mat-sys-surface-variant));
  margin-bottom: var(--mat-menu-divider-bottom-spacing, 8px);
  margin-top: var(--mat-menu-divider-top-spacing, 8px);
}

.mat-mdc-menu-item {
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  padding: 0;
  cursor: pointer;
  width: 100%;
  text-align: left;
  box-sizing: border-box;
  color: inherit;
  font-size: inherit;
  background: none;
  text-decoration: none;
  margin: 0;
  min-height: 48px;
  padding-left: var(--mat-menu-item-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-trailing-spacing, 12px);
  -webkit-user-select: none;
  user-select: none;
  cursor: pointer;
  outline: none;
  border: none;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-menu-item::-moz-focus-inner {
  border: 0;
}
[dir=rtl] .mat-mdc-menu-item {
  padding-left: var(--mat-menu-item-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-leading-spacing, 12px);
}
.mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-leading-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-item:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding-left: var(--mat-menu-item-with-icon-trailing-spacing, 12px);
  padding-right: var(--mat-menu-item-with-icon-leading-spacing, 12px);
}
.mat-mdc-menu-item, .mat-mdc-menu-item:visited, .mat-mdc-menu-item:link {
  color: var(--mat-menu-item-label-text-color, var(--mat-sys-on-surface));
}
.mat-mdc-menu-item .mat-icon-no-color,
.mat-mdc-menu-item .mat-mdc-menu-submenu-icon {
  color: var(--mat-menu-item-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-menu-item[disabled] {
  cursor: default;
  opacity: 0.38;
}
.mat-mdc-menu-item[disabled]::after {
  display: block;
  position: absolute;
  content: "";
  top: 0;
  left: 0;
  bottom: 0;
  right: 0;
}
.mat-mdc-menu-item:focus {
  outline: 0;
}
.mat-mdc-menu-item .mat-icon {
  flex-shrink: 0;
  margin-right: var(--mat-menu-item-spacing, 12px);
  height: var(--mat-menu-item-icon-size, 24px);
  width: var(--mat-menu-item-icon-size, 24px);
}
[dir=rtl] .mat-mdc-menu-item {
  text-align: right;
}
[dir=rtl] .mat-mdc-menu-item .mat-icon {
  margin-right: 0;
  margin-left: var(--mat-menu-item-spacing, 12px);
}
.mat-mdc-menu-item:not([disabled]):hover {
  background-color: var(--mat-menu-item-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-menu-item:not([disabled]).cdk-program-focused, .mat-mdc-menu-item:not([disabled]).cdk-keyboard-focused, .mat-mdc-menu-item:not([disabled]).mat-mdc-menu-item-highlighted {
  background-color: var(--mat-menu-item-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
}
@media (forced-colors: active) {
  .mat-mdc-menu-item {
    margin-top: 1px;
  }
}

.mat-mdc-menu-submenu-icon {
  width: var(--mat-menu-item-icon-size, 24px);
  height: 10px;
  fill: currentColor;
  padding-left: var(--mat-menu-item-spacing, 12px);
}
[dir=rtl] .mat-mdc-menu-submenu-icon {
  padding-right: var(--mat-menu-item-spacing, 12px);
  padding-left: 0;
}
[dir=rtl] .mat-mdc-menu-submenu-icon polygon {
  transform: scaleX(-1);
  transform-origin: center;
}
@media (forced-colors: active) {
  .mat-mdc-menu-submenu-icon {
    fill: CanvasText;
  }
}

.mat-mdc-menu-item .mat-mdc-menu-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
`],encapsulation:2})}return t})(),NF=new y("mat-menu-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(ae);return()=>xo(t)}});var Bs=new WeakMap,kF=(()=>{class t{_canHaveBackdrop;_element=u(j);_viewContainerRef=u(Rt);_menuItemInstance=u($c,{optional:!0,self:!0});_dir=u(An,{optional:!0});_focusMonitor=u(fr);_ngZone=u(O);_injector=u(ae);_scrollStrategy=u(NF);_changeDetectorRef=u(me);_animationsDisabled=St();_portal;_overlayRef=null;_menuOpen=!1;_closingActionsSubscription=fe.EMPTY;_menuCloseSubscription=fe.EMPTY;_pendingRemoval;_parentMaterialMenu;_parentInnerPadding;_openedBy=void 0;get _menu(){return this._menuInternal}set _menu(e){e!==this._menuInternal&&(this._menuInternal=e,this._menuCloseSubscription.unsubscribe(),e?(this._parentMaterialMenu,this._menuCloseSubscription=e.close.subscribe(i=>{this._destroyMenu(i),(i==="click"||i==="tab")&&this._parentMaterialMenu&&this._parentMaterialMenu.closed.emit(i)})):this._destroyMenu(),this._menuItemInstance?._setTriggersSubmenu(this._triggersSubmenu()))}_menuInternal=null;constructor(e){this._canHaveBackdrop=e;let i=u(_v,{optional:!0});this._parentMaterialMenu=i instanceof Us?i:void 0}ngOnDestroy(){this._menu&&this._ownsMenu(this._menu)&&Bs.delete(this._menu),this._pendingRemoval?.unsubscribe(),this._menuCloseSubscription.unsubscribe(),this._closingActionsSubscription.unsubscribe(),this._overlayRef&&(this._overlayRef.dispose(),this._overlayRef=null)}get menuOpen(){return this._menuOpen}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}_triggersSubmenu(){return!!(this._menuItemInstance&&this._parentMaterialMenu&&this._menu)}_closeMenu(){this._menu?.close.emit()}_openMenu(e){if(this._triggerIsAriaDisabled())return;let i=this._menu;if(this._menuOpen||!i)return;this._pendingRemoval?.unsubscribe();let r=Bs.get(i);Bs.set(i,this),r&&r!==this&&r._closeMenu();let o=this._createOverlay(i),s=o.getConfig(),a=s.positionStrategy;this._setPosition(i,a),this._canHaveBackdrop?s.hasBackdrop=i.hasBackdrop==null?!this._triggersSubmenu():i.hasBackdrop:s.hasBackdrop=i.hasBackdrop??!1,o.hasAttached()||(o.attach(this._getPortal(i)),i.lazyContent?.attach(this.menuData)),this._closingActionsSubscription=this._menuClosingActions().subscribe(()=>this._closeMenu()),i.parentMenu=this._triggersSubmenu()?this._parentMaterialMenu:void 0,i.direction=this.dir,e&&i.focusFirstItem(this._openedBy||"program"),this._setIsMenuOpen(!0),i instanceof Us&&(i._setIsOpen(!0),i._directDescendantItems.changes.pipe(ke(i.close)).subscribe(()=>{a.withLockedPosition(!1).reapplyLastPosition(),a.withLockedPosition(!0)}))}focus(e,i){this._focusMonitor&&e?this._focusMonitor.focusVia(this._element,e,i):this._element.nativeElement.focus(i)}_destroyMenu(e){let i=this._overlayRef,r=this._menu;!i||!this.menuOpen||(this._closingActionsSubscription.unsubscribe(),this._pendingRemoval?.unsubscribe(),r instanceof Us&&this._ownsMenu(r)?(this._pendingRemoval=r._animationDone.pipe(Gt(1)).subscribe(()=>{i.detach(),Bs.has(r)||r.lazyContent?.detach()}),r._setIsOpen(!1)):(i.detach(),r?.lazyContent?.detach()),r&&this._ownsMenu(r)&&Bs.delete(r),this.restoreFocus&&(e==="keydown"||!this._openedBy||!this._triggersSubmenu())&&this.focus(this._openedBy),this._openedBy=void 0,this._setIsMenuOpen(!1))}_setIsMenuOpen(e){e!==this._menuOpen&&(this._menuOpen=e,this._menuOpen?this.menuOpened.emit():this.menuClosed.emit(),this._triggersSubmenu()&&this._menuItemInstance._setHighlighted(e),this._changeDetectorRef.markForCheck())}_createOverlay(e){if(!this._overlayRef){let i=this._getOverlayConfig(e);this._subscribeToPositions(e,i.positionStrategy),this._overlayRef=Uc(this._injector,i),this._overlayRef.keydownEvents().subscribe(r=>{this._menu instanceof Us&&this._menu._handleKeydown(r)})}return this._overlayRef}_getOverlayConfig(e){return new Do({positionStrategy:jc(this._injector,this._getOverlayOrigin()).withLockedPosition().withGrowAfterOpen().withTransformOriginOn(".mat-menu-panel, .mat-mdc-menu-panel"),backdropClass:e.backdropClass||"cdk-overlay-transparent-backdrop",panelClass:e.overlayPanelClass,scrollStrategy:this._scrollStrategy(),direction:this._dir||"ltr",disableAnimations:this._animationsDisabled})}_subscribeToPositions(e,i){e.setPositionClasses&&i.positionChanges.subscribe(r=>{this._ngZone.run(()=>{let o=r.connectionPair.overlayX==="start"?"after":"before",s=r.connectionPair.overlayY==="top"?"below":"above";e.setPositionClasses(o,s)})})}_setPosition(e,i){let[r,o]=e.xPosition==="before"?["end","start"]:["start","end"],[s,a]=e.yPosition==="above"?["bottom","top"]:["top","bottom"],[c,l]=[s,a],[d,f]=[r,o],m=0;if(this._triggersSubmenu()){if(f=r=e.xPosition==="before"?"start":"end",o=d=r==="end"?"start":"end",this._parentMaterialMenu){if(this._parentInnerPadding==null){let g=this._parentMaterialMenu.items.first;this._parentInnerPadding=g?g._getHostElement().offsetTop:0}m=s==="bottom"?this._parentInnerPadding:-this._parentInnerPadding}}else e.overlapTrigger||(c=s==="top"?"bottom":"top",l=a==="top"?"bottom":"top");i.withPositions([{originX:r,originY:c,overlayX:d,overlayY:s,offsetY:m},{originX:o,originY:c,overlayX:f,overlayY:s,offsetY:m},{originX:r,originY:l,overlayX:d,overlayY:a,offsetY:-m},{originX:o,originY:l,overlayX:f,overlayY:a,offsetY:-m}])}_menuClosingActions(){let e=this._getOutsideClickStream(this._overlayRef),i=this._overlayRef.detachments(),r=this._parentMaterialMenu?this._parentMaterialMenu.closed:L(),o=this._parentMaterialMenu?this._parentMaterialMenu._hovered().pipe(Se(s=>this._menuOpen&&s!==this._menuItemInstance)):L();return Qt(e,r,o,i)}_getPortal(e){return(!this._portal||this._portal.templateRef!==e.templateRef)&&(this._portal=new Eo(e.templateRef,this._viewContainerRef)),this._portal}_ownsMenu(e){return Bs.get(e)===this}_triggerIsAriaDisabled(){return B(this._element.nativeElement.getAttribute("aria-disabled"))}static \u0275fac=function(i){ka()};static \u0275dir=k({type:t})}return t})(),ID=(()=>{class t extends kF{_cleanupTouchstart;_hoverSubscription=fe.EMPTY;get _deprecatedMatMenuTriggerFor(){return this.menu}set _deprecatedMatMenuTriggerFor(e){this.menu=e}get menu(){return this._menu}set menu(e){this._menu=e}menuData;restoreFocus=!0;menuOpened=new Y;onMenuOpen=this.menuOpened;menuClosed=new Y;onMenuClose=this.menuClosed;constructor(){super(!0);let e=u(je);this._cleanupTouchstart=e.listen(this._element.nativeElement,"touchstart",i=>{go(i)||(this._openedBy="touch")},{passive:!0})}triggersSubmenu(){return super._triggersSubmenu()}toggleMenu(){return this.menuOpen?this.closeMenu():this.openMenu()}openMenu(){this._openMenu(!0)}closeMenu(){this._closeMenu()}updatePosition(){this._overlayRef?.updatePosition()}ngAfterContentInit(){this._handleHover()}ngOnDestroy(){super.ngOnDestroy(),this._cleanupTouchstart(),this._hoverSubscription.unsubscribe()}_getOverlayOrigin(){return this._element}_getOutsideClickStream(e){return e.backdropClick()}_handleMousedown(e){mo(e)||(this._openedBy=e.button===0?"mouse":void 0,this.triggersSubmenu()&&e.preventDefault())}_handleKeydown(e){let i=e.keyCode;(i===13||i===32)&&(this._openedBy="keyboard"),this.triggersSubmenu()&&(i===39&&this.dir==="ltr"||i===37&&this.dir==="rtl")&&(this._openedBy="keyboard",this.openMenu())}_handleClick(e){this.triggersSubmenu()?(e.stopPropagation(),this.openMenu()):this.toggleMenu()}_handleHover(){this.triggersSubmenu()&&this._parentMaterialMenu&&(this._hoverSubscription=this._parentMaterialMenu._hovered().subscribe(e=>{e===this._menuItemInstance&&!e.disabled&&this._parentMaterialMenu?._panelAnimationState!=="void"&&(this._openedBy="mouse",this._openMenu(!1))}))}static \u0275fac=function(i){return new(i||t)};static \u0275dir=k({type:t,selectors:[["","mat-menu-trigger-for",""],["","matMenuTriggerFor",""]],hostAttrs:[1,"mat-mdc-menu-trigger"],hostVars:3,hostBindings:function(i,r){i&1&&F("click",function(s){return r._handleClick(s)})("mousedown",function(s){return r._handleMousedown(s)})("keydown",function(s){return r._handleKeydown(s)}),i&2&&he("aria-haspopup",r.menu?"menu":null)("aria-expanded",r.menuOpen)("aria-controls",r.menuOpen?r.menu?.panelId:null)},inputs:{_deprecatedMatMenuTriggerFor:[0,"mat-menu-trigger-for","_deprecatedMatMenuTriggerFor"],menu:[0,"matMenuTriggerFor","menu"],menuData:[0,"matMenuTriggerData","menuData"],restoreFocus:[0,"matMenuTriggerRestoreFocus","restoreFocus"]},outputs:{menuOpened:"menuOpened",onMenuOpen:"onMenuOpen",menuClosed:"menuClosed",onMenuClose:"onMenuClose"},exportAs:["matMenuTrigger"],features:[Be]})}return t})();var SD=(()=>{class t{static \u0275fac=function(i){return new(i||t)};static \u0275mod=q({type:t});static \u0275inj=z({imports:[hr,Hc,Ie,Fs]})}return t})();var Hs=class t{constructor(n){this.http=n}http;todas(){return this.http.get(`${Ge.apiUrl}/empresa/all`)}porId(n){return this.http.get(`${Ge.apiUrl}/empresa/${n}`)}porNome(n){return this.http.get(`${Ge.apiUrl}/empresa`,{params:{nome:n}})}criar(n){return this.http.post(`${Ge.apiUrl}/empresa`,n)}atualizar(n){return this.http.put(`${Ge.apiUrl}/empresa`,n)}remover(n){return this.http.delete(`${Ge.apiUrl}/empresa/${n}`)}static \u0275fac=function(e){return new(e||t)(M(Sn))};static \u0275prov=V({token:t,factory:t.\u0275fac,providedIn:"root"})};var FF=t=>["/empresas",t];function PF(t,n){if(t&1){let e=Ue();p(0,"button",12),F("click",function(){le(e);let r=w();return r.termoBusca="",de(r.filtrarEmpresas())}),p(1,"mat-icon"),_(2,"close"),h()()}}function LF(t,n){if(t&1&&(p(0,"div",13),_(1),h()),t&2){let e=w();v(),be(" ",e.mensagens," ")}}function VF(t,n){t&1&&(p(0,"div",14),pe(1,"mat-spinner",15),h())}function jF(t,n){t&1&&(p(0,"div",26),_(1," Usu\xE1rios relacionados "),h())}function BF(t,n){if(t&1&&(p(0,"button",27)(1,"mat-icon"),_(2,"email"),h(),p(3,"span"),_(4),h()()),t&2){let e=n.$implicit;v(4),nt(e.email)}}function UF(t,n){t&1&&(p(0,"div",28),_(1," Nenhum usu\xE1rio vinculado "),h())}function HF(t,n){if(t&1){let e=Ue();p(0,"tr")(1,"td"),_(2),h(),p(3,"td",19)(4,"button",20)(5,"mat-icon"),_(6,"group"),h()(),p(7,"mat-menu",null,0),$(9,jF,2,0,"div",21)(10,BF,5,1,"button",22)(11,UF,2,0,"div",23),h(),p(12,"button",24)(13,"mat-icon"),_(14,"edit"),h()(),p(15,"button",25),F("click",function(){let r=le(e).$implicit,o=w(2);return de(o.remover(r.id))}),p(16,"mat-icon"),_(17,"delete"),h()()()()}if(t&2){let e=n.$implicit,i=ri(8);v(2),nt(e.nome),v(2),E("matMenuTriggerFor",i),v(5),E("ngIf",e.usuarios?.length),v(),E("ngForOf",e.usuarios),v(),E("ngIf",!e.usuarios?.length),v(),E("routerLink",oo(6,FF,e.id))}}function zF(t,n){if(t&1&&(p(0,"table",16)(1,"thead")(2,"tr")(3,"th"),_(4,"Nome"),h(),p(5,"th",17),_(6,"A\xE7\xF5es"),h()()(),p(7,"tbody"),$(8,HF,18,8,"tr",18),h()()),t&2){let e=w();v(8),E("ngForOf",e.empresasFiltradas)}}function $F(t,n){t&1&&(p(0,"p"),_(1,"Clique em "),p(2,"strong"),_(3,"Nova Empresa"),h(),_(4," para come\xE7ar."),h())}function GF(t,n){t&1&&(p(0,"p"),_(1,"Tente outro termo de busca."),h())}function WF(t,n){if(t&1&&(p(0,"div",29)(1,"mat-icon",30),_(2," group "),h(),p(3,"h3"),_(4),h(),$(5,$F,5,0,"p",31)(6,GF,2,0,"p",31),h()),t&2){let e=w();v(4),nt((e.termoBusca,"Nenhuma empresa encontrada")),v(),E("ngIf",!e.termoBusca),v(),E("ngIf",e.termoBusca)}}var Sf=class t{constructor(n,e){this.empresaService=n;this.cdr=e}empresaService;cdr;empresas=[];carregando=!0;mensagens="";empresasFiltradas=[];termoBusca="";get isAdmin(){return localStorage.getItem("role")==="ADMIN"}ngOnInit(){this.carregar()}carregar(){this.carregando=!0,this.empresaService.todas().subscribe({next:n=>{console.log(n),this.empresas=n,this.filtrarEmpresas(),console.log(this.empresasFiltradas),this.carregando=!1,this.cdr.detectChanges()},error:n=>{this.mensagens="Erro ao carregar empresas.",this.carregando=!1,this.cdr.detectChanges()}})}filtrarEmpresas(){let n=this.termoBusca.trim().toLowerCase();if(!n){this.empresasFiltradas=this.empresas;return}this.empresasFiltradas=this.empresas.filter(e=>e.nome?.toLowerCase().includes(n))}remover(n){n!=null&&confirm("Confirma\xE7ao para remover esta empresa")&&this.empresaService.remover(n).subscribe({next:()=>{this.empresas=this.empresas.filter(e=>e.id!==n),this.filtrarEmpresas(),this.cdr.detectChanges()},error:e=>{console.error("Erro ao remover empresa",e),this.mensagens="Nao foi possivel remover esta empresa.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(x(Hs),x(me))};static \u0275cmp=R({type:t,selectors:[["app-gestao-empresas"]],decls:20,vars:6,consts:[["menuUsuariosRelacionados","matMenu"],[1,"page"],[1,"header"],["mat-raised-button","","color","primary","routerLink","/empresas/nova"],["appearance","outline",1,"search-field"],["matInput","","placeholder","Nome da empresa...",3,"ngModelChange","ngModel"],["matPrefix",""],["mat-icon-button","","matSuffix","",3,"click",4,"ngIf"],["class","error",4,"ngIf"],["class","loading",4,"ngIf"],["class","mat-elevation-z2",4,"ngIf"],["class","empty-state",4,"ngIf"],["mat-icon-button","","matSuffix","",3,"click"],[1,"error"],[1,"loading"],["diameter","45"],[1,"mat-elevation-z2"],[2,"width","170px"],[4,"ngFor","ngForOf"],[1,"actions"],["mat-icon-button","","color","accent","matTooltip","Usu\xE1rios relacionados",3,"matMenuTriggerFor"],["class","menu-header",4,"ngIf"],["mat-menu-item","",4,"ngFor","ngForOf"],["class","menu-empty",4,"ngIf"],["mat-icon-button","","color","primary",3,"routerLink"],["mat-icon-button","","color","warn",3,"click"],[1,"menu-header"],["mat-menu-item",""],[1,"menu-empty"],[1,"empty-state"],[1,"empty-icon"],[4,"ngIf"]],template:function(e,i){e&1&&(p(0,"div",1)(1,"mat-card")(2,"div",2)(3,"h2"),_(4,"Gest\xE3o de Empresas"),h(),p(5,"button",3)(6,"mat-icon"),_(7,"add"),h(),_(8," Nova Empresa "),h()(),p(9,"mat-form-field",4)(10,"mat-label"),_(11,"Pesquisar"),h(),p(12,"input",5),Ne("ngModelChange",function(o){return Pe(i.termoBusca,o)||(i.termoBusca=o),o}),F("ngModelChange",function(){return i.filtrarEmpresas()}),h(),Oe(),p(13,"mat-icon",6),_(14,"search"),h(),$(15,PF,3,0,"button",7),h(),$(16,LF,2,1,"div",8)(17,VF,2,0,"div",9)(18,zF,9,1,"table",10)(19,WF,7,3,"div",11),h()()),e&2&&(v(12),Re("ngModel",i.termoBusca),Fe(),v(3),E("ngIf",i.termoBusca),v(),E("ngIf",i.mensagens),v(),E("ngIf",i.carregando),v(),E("ngIf",!i.carregando&&i.empresasFiltradas.length>0),v(),E("ngIf",!i.carregando&&i.empresasFiltradas.length===0))},dependencies:[Ft,pt,Ot,Dt,rt,it,tn,ct,Lt,Pt,gt,Vt,Fi,Ht,Ut,Fn,On,Je,vt,lt,Co,fn,Bt,jt,SD,Us,$c,ID,Os],styles:[".mat-mdc-chip[_ngcontent-%COMP%]{font-weight:600}.actions[_ngcontent-%COMP%]{display:flex;gap:8px}.empty-state[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;text-align:center;padding:60px 20px;opacity:.8}.empty-icon[_ngcontent-%COMP%]{font-size:70px!important;width:70px!important;height:70px!important;color:var(--mat-sys-primary);margin-bottom:16px}"]})};function qF(t,n){t&1&&(p(0,"div",3),pe(1,"mat-spinner",4),h())}function YF(t,n){if(t&1&&(p(0,"div",20),_(1),h()),t&2){let e=w(2);v(),be(" ",e.erroUsuario," ")}}function ZF(t,n){if(t&1){let e=Ue();p(0,"mat-chip",21),F("removed",function(){let r=le(e).$implicit,o=w(2);return de(o.removerUsuario(r.id))}),_(1),p(2,"button",22)(3,"mat-icon"),_(4,"cancel"),h()()()}if(t&2){let e=n.$implicit;v(),be(" ",e.email," ")}}function KF(t,n){t&1&&(p(0,"p",23),_(1," Nenhum usu\xE1rio vinculado ainda. "),h())}function QF(t,n){if(t&1&&(p(0,"div",20),_(1),h()),t&2){let e=w(2);v(),be(" ",e.mensagens," ")}}function XF(t,n){if(t&1){let e=Ue();p(0,"form",5),F("ngSubmit",function(){le(e);let r=w();return de(r.onSubmit())})("keydown.enter",function(r){le(e);let o=w();return r.preventDefault(),de(o.onSubmit())}),p(1,"mat-form-field",6)(2,"mat-label"),_(3,"Nome"),h(),p(4,"input",7),Ne("ngModelChange",function(r){le(e);let o=w();return Pe(o.empresa.nome,r)||(o.empresa.nome=r),de(r)}),h(),Oe(),h(),p(5,"div",8)(6,"h3"),_(7,"Usu\xE1rios relacionados"),h(),p(8,"div",9)(9,"mat-form-field",10)(10,"mat-label"),_(11,"E-mail do usu\xE1rio"),h(),p(12,"input",11),Ne("ngModelChange",function(r){le(e);let o=w();return Pe(o.novoEmail,r)||(o.novoEmail=r),de(r)}),F("keydown.enter",function(r){le(e);let o=w();return r.preventDefault(),de(o.adicionarUsuario())}),h(),Oe(),h(),p(13,"button",12),F("click",function(){le(e);let r=w();return de(r.adicionarUsuario())}),p(14,"mat-icon"),_(15,"add_circle"),h()()(),$(16,YF,2,1,"div",13),p(17,"mat-chip-set",14),$(18,ZF,5,1,"mat-chip",15),h(),$(19,KF,2,0,"p",16),h(),$(20,QF,2,1,"div",13),p(21,"div",17)(22,"button",18)(23,"mat-icon"),_(24,"save"),h(),_(25," Salvar "),h(),p(26,"button",19)(27,"mat-icon"),_(28,"arrow_back"),h(),_(29," Voltar "),h()()()}if(t&2){let e=w();v(4),Re("ngModel",e.empresa.nome),Fe(),v(8),Re("ngModel",e.novoEmail),Fe(),v(4),E("ngIf",e.erroUsuario),v(2),E("ngForOf",e.empresa.usuarios),v(),E("ngIf",!e.empresa.usuarios?.length),v(),E("ngIf",e.mensagens)}}var Gc=class t{constructor(n,e,i,r,o){this.empresaService=n;this.usuarioService=e;this.route=i;this.router=r;this.cdr=o}empresaService;usuarioService;route;router;cdr;empresa={nome:"",usuarios:[]};modoEdicao=!1;carregando=!1;mensagens="";novoEmail="";erroUsuario="";ngOnInit(){let n=this.route.snapshot.paramMap.get("id");n&&!isNaN(Number(n))&&(this.modoEdicao=!0,this.carregando=!0,this.empresaService.porId(Number(n)).subscribe({next:e=>{this.empresa=e,this.empresa.usuarios||(this.empresa.usuarios=[]),this.carregando=!1,this.cdr.detectChanges()},error:e=>{console.error("Erro ao carregar empresa",e),this.mensagens="Empresa nao encontrada.",this.carregando=!1,this.cdr.detectChanges()}}))}adicionarUsuario(){let n=this.novoEmail.trim();if(!n)return;if(this.empresa.usuarios?.some(i=>i.email.toLowerCase()===n.toLowerCase())){this.erroUsuario="Usu\xE1rio j\xE1 adicionado.";return}this.usuarioService.porEmail(n).subscribe({next:i=>{this.empresa.usuarios||(this.empresa.usuarios=[]),this.empresa.usuarios.push(i),this.novoEmail="",this.erroUsuario="",this.cdr.detectChanges()},error:i=>{console.error("Erro ao buscar usu\xE1rio",i),this.erroUsuario="Usu\xE1rio n\xE3o encontrado.",this.cdr.detectChanges()}})}removerUsuario(n){this.empresa.usuarios=this.empresa.usuarios?.filter(e=>e.id!==n)}onSubmit(){(this.modoEdicao?this.empresaService.atualizar(this.empresa):this.empresaService.criar(this.empresa)).subscribe({next:()=>{this.router.navigate(["/empresas"])},error:e=>{console.error("Erro ao salvar empresa",e),this.mensagens=e.error?.erro??e.status===403?"Sem permissao para essa acao.":"Nao foi possivel salvar a empresa.",this.cdr.detectChanges()}})}static \u0275fac=function(e){return new(e||t)(x(Hs),x(_r),x(Yt),x(Ve),x(me))};static \u0275cmp=R({type:t,selectors:[["app-empresa-form"]],decls:6,vars:3,consts:[[1,"page"],["class","loading",4,"ngIf"],[3,"ngSubmit","keydown.enter",4,"ngIf"],[1,"loading"],["diameter","45"],[3,"ngSubmit","keydown.enter"],["appearance","outline"],["matInput","","id","nome","name","nome","required","",3,"ngModelChange","ngModel"],[1,"usuarios-section"],[1,"add-usuario"],["appearance","outline",1,"email-field"],["matInput","","name","novoEmail","placeholder","usuario@email.com",3,"ngModelChange","keydown.enter","ngModel"],["mat-icon-button","","color","primary","type","button",3,"click"],["class","error",4,"ngIf"],["aria-label","Usu\xE1rios relacionados"],[3,"removed",4,"ngFor","ngForOf"],["class","empty-usuarios",4,"ngIf"],[1,"actions"],["mat-raised-button","","color","primary","type","submit"],["mat-stroked-button","","routerLink","/empresas","type","button"],[1,"error"],[3,"removed"],["matChipRemove",""],[1,"empty-usuarios"]],template:function(e,i){e&1&&(p(0,"div",0)(1,"mat-card")(2,"h2"),_(3),h(),$(4,qF,2,0,"div",1)(5,XF,30,6,"form",2),h()()),e&2&&(v(3),be(" ",i.modoEdicao?"\u270F\uFE0F Editar Empresa":"Nova Empresa"," "),v(),E("ngIf",i.carregando),v(),E("ngIf",!i.carregando))},dependencies:[it,tn,ct,Ft,di,pt,Ot,li,Tn,Dt,Zt,rt,Lt,Pt,Je,vt,lt,Bt,jt,Ls,gt,Vt,Fi,Ht,Ut,Fn,On,Os,Oc,Xw,df],styles:[".usuarios-section[_ngcontent-%COMP%]{margin:16px 0}.add-usuario[_ngcontent-%COMP%]{display:flex;align-items:center;gap:8px}.email-field[_ngcontent-%COMP%]{flex:1}.empty-usuarios[_ngcontent-%COMP%]{color:#0006;font-style:italic;font-size:14px}"]})};var TD=[{path:"",redirectTo:"login",pathMatch:"full"},{path:"login",component:cf,canActivate:[gv]},{path:"criar-usuario",component:lf,canActivate:[gv]},{path:"pets",component:Df,canActivate:[js]},{path:"pets/novo",component:zc,canActivate:[js]},{path:"pets/:id",component:zc,canActivate:[js]},{path:"usuarios",component:uf,canActivate:[js]},{path:"usuarios/:id",component:wf,canActivate:[js]},{path:"empresas",component:Sf,canActivate:[xf]},{path:"empresas/novo",component:Gc,canActivate:[xf]},{path:"empresas/:id",component:Gc,canActivate:[xf]}];var AD=(t,n)=>{let e=localStorage.getItem("token");return e&&(t=t.clone({setHeaders:{Authorization:`Bearer ${e}`}})),n(t)};var RD={providers:[Qh(),Ag(TD),ig(rg([AD]))]};function JF(t,n){if(t&1&&(p(0,"span",11)(1,"mat-icon"),_(2,"account_circle"),h(),_(3),h()),t&2){let e=w(2);v(3),be(" ",e.emailUsuario," ")}}function eP(t,n){if(t&1){let e=Ue();p(0,"nav",1)(1,"div",2),pe(2,"span",3),h(),p(3,"div",4)(4,"a",5),_(5,"Pets"),h(),p(6,"a",6),_(7,"Usu\xE1rios"),h(),p(8,"a",7),_(9,"Empresas"),h()(),p(10,"div",8),$(11,JF,4,1,"span",9),p(12,"button",10),F("click",function(){le(e);let r=w();return de(r.sair())}),p(13,"mat-icon"),_(14,"logout"),h(),_(15," Sair "),h()()()}if(t&2){let e=w();v(11),E("ngIf",e.logado)}}var Mf=class t{constructor(n,e){this.authService=n;this.router=e}authService;router;get logado(){return this.authService.isLoggedIn()}get emailUsuario(){return localStorage.getItem("email")}sair(){this.authService.logout(),this.router.navigate(["/login"])}get isAdmin(){return localStorage.getItem("role")==="ADMIN"}static \u0275fac=function(e){return new(e||t)(x(kn),x(Ve))};static \u0275cmp=R({type:t,selectors:[["app-root"]],decls:2,vars:1,consts:[["class","top-nav",4,"ngIf"],[1,"top-nav"],[1,"nav-left"],[1,"logo"],[1,"nav-center"],["routerLink","/pets","routerLinkActive","active"],["routerLink","/usuarios","routerLinkActive","active"],["routerLink","/empresas","routerLinkActive","active"],[1,"nav-right"],["class","nav-user",4,"ngIf"],["mat-raised-button","","color","primary",3,"click"],[1,"nav-user"]],template:function(e,i){e&1&&($(0,eP,16,1,"nav",0),pe(1,"router-outlet")),e&2&&E("ngIf",i.logado)},dependencies:[dc,it,ct,rt,Ht,Ut,gt,Vt],encapsulation:2})};Ym(Mf,RD).catch(t=>console.error(t));
